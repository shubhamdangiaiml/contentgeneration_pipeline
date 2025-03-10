import os
import json
import base64
import logging
from datetime import datetime, timedelta
import requests
import pymongo
import google.generativeai as genai
from PIL import Image
import io
from gridfs import GridFS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.getLogger("pymongo").setLevel(logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = "mongodb+srv://moreyeahsaimldatascience:WMelEMakMwCiPygO@aimlmoreyeahs.8vjae.mongodb.net/?retryWrites=true&w=majority&appName=aimlmoreyeahs"
DB_NAME = "Marketing_data"
COMPANY_COLLECTION = "company_details"

HF_API_URL_FLUX = "https://api-inference.huggingface.co/models/black-forest-labs/FLUX.1-dev"
HF_API_URL_MIDJOURNEY = "https://api-inference.huggingface.co/models/Jovie/Midjourney"
HF_HEADERS = {"Authorization": "Bearer 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"}

GEMINI_API_KEY = 'AIzaSyB9YriqATKbxNWoeeRh8EGmiMztrAIGtJ4'

class MarketingContentGenerator:
    def __init__(self):
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        genai.configure(api_key=GEMINI_API_KEY)
        self.images_dir = os.path.join(os.getcwd(), 'images')
        os.makedirs(self.images_dir, exist_ok=True)
        logger.info("Marketing Content Generator initialized")

    def parse_company_data(self, company_doc):
        """Parse and validate company document data"""
        try:
            # convert data in json from str
            if isinstance(company_doc, str):
                company_data = json.loads(company_doc)
            else:
                company_data = company_doc

            # Ensure company_data is a dictionary
            if not isinstance(company_data, dict):
                raise ValueError(f"Invalid company data format: {type(company_data)}")

            # Set default values for required fields
            company_data = {
                'company_name': company_data.get('company_name', 'Unknown Company'),
                'business_domain': company_data.get('business_domain', 'It'),
                'specific_focus': company_data.get('specific_focus', 'Sales'),
                'target_audience': company_data.get('target_audience', 'Startup'),
                'key_features': company_data.get('key_features', 'Low Price'),
                'unique_selling_points': company_data.get('unique_selling_points', 'Software'),
                'pricing_packages': company_data.get('pricing_packages', ''),
                'target_platform': company_data.get('target_platform', ['Facebook']),
                'products_or_services': company_data.get('products_or_services', []),
                'days': int(company_data.get('days', 1)),
                'posting_schedule': company_data.get('posting_schedule', {'type': 'daily'}),
                'logo_id': company_data.get('logo_id', '')
            }

            # Convert str to list
            if isinstance(company_data['target_platform'], str):
                company_data['target_platform'] = [p.strip() for p in company_data['target_platform'].split(",") if p.strip()]
            
            if isinstance(company_data['products_or_services'], str):
                company_data['products_or_services'] = [p.strip() for p in company_data['products_or_services'].split(",") if p.strip()]
            
            # Ensure posting_schedule is a dictionary
            if isinstance(company_data['posting_schedule'], str):
                try:
                    company_data['posting_schedule'] = json.loads(company_data['posting_schedule'])
                except json.JSONDecodeError:
                    company_data['posting_schedule'] = {'type': 'daily'}

            return company_data
        except Exception as e:
            logger.error(f"Error parsing company data: {e}")
            return None

    def query_huggingface(self, payload, api_url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.post(api_url, headers=HF_HEADERS, json=payload)
                if response.status_code == 200:
                    return response
                if response.status_code == 503:
                    logger.warning(f"Service temporarily unavailable for {api_url}")
                    if api_url == HF_API_URL_FLUX:
                        logger.info("Switching to Midjourney model...")
                        return None
                    continue
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"API call failed for {api_url}: {str(e)}")
                if api_url == HF_API_URL_FLUX:
                    logger.info("Switching to Midjourney model...")
                    return None
                continue
        return None

    def get_last_content_date(self, company_name):
        try:
            company_collection_name = f"marketing_content_{company_name.lower().replace(' ', '_')}"
            last_record = self.db[company_collection_name].find_one(
                sort=[('content_date', pymongo.DESCENDING)]
            )
            if last_record:
                logger.info(f"Last content date for {company_name}: {last_record['content_date'].strftime('%Y-%m-%d')}")
            else:
                logger.info(f"No existing content found for {company_name}")
            return last_record['content_date'] if last_record else None
        except Exception as e:
            logger.error(f"Error fetching last content date for {company_name}: {e}")
            return None

    def needs_content_generation(self, company_name, last_content_date, current_date):
        if not last_content_date:
            logger.info(f"Starting fresh content generation for {company_name}")
            return True
            
        if last_content_date.date() >= current_date.date():
            logger.info(f"Content already exists up to current date for {company_name}. Skipping company.")
            return False
            
        logger.info(f"Generating additional content for {company_name} starting after {last_content_date.strftime('%Y-%m-%d')}")
        return True

    def should_generate_content(self, posting_schedule, target_date):
        try:
            if isinstance(posting_schedule, str):
                posting_schedule = json.loads(posting_schedule)
        except json.JSONDecodeError:
            logger.warning(f"Error parsing posting_schedule. Using default settings.")
            posting_schedule = {"type": "daily"}

        schedule_type = posting_schedule.get('type', 'daily')

        if schedule_type == 'daily':
            return True

        if schedule_type == 'specific_days':
            posting_days = posting_schedule.get('days', [])
            return target_date.strftime('%A') in posting_days

        return False

    def generate_marketing_content(self, company_data, product):
        company_name = company_data.get('company_name', 'Unknown Company')
        try:
            logger.info(f"Generating marketing content for {company_name} - Product: {product}")
            model = genai.GenerativeModel('gemini-1.5-flash')
            input_prompt = f"""
            Generate unique marketing content for:
            - Company Name: {company_name}
            - Business Domain: {company_data.get('business_domain', 'Not Specified')}
            - Specific Focus: {company_data.get('specific_focus', 'General')}
            - Target Audience: {company_data.get('target_audience', 'All')}
            - Key Features: {company_data.get('key_features', 'Not Defined')}
            - Unique Selling Points: {company_data.get('unique_selling_points', 'Unique Value')}
            - Pricing & Packages: {company_data.get('pricing_packages', 'Competitive')}
            - Target Platform: {company_data.get('target_platform', 'Multi-platform')}
            - Product/Service: {product}

            Provide a JSON response with:
            - Title: Marketing title
            - Punchline: Catchy phrase
            - Content: 125-word description
            - Hashtags: 5 relevant hashtags
            - Keywords: 5 key descriptors
            Important: Avoid using any special characters like *, _, or other markdown symbols. Provide plain text only
            Important-Generate content according this-
            "Twitter": "Ensure the full content is below 250 characters.",
            "LinkedIn": "Content should be professional and detailed (up to 125 words).",
            "Instagram": "Make the content engaging.",
            "Facebook": "Create balanced content suitable for a broad audience.",
            """
            completion = model.generate_content([input_prompt])
            response_text = completion.text.strip()
            if response_text.startswith('```json'):
                response_text = response_text.replace('```json', '').strip()
            if response_text.endswith('```'):
                response_text = response_text[:-3].strip()
            content = json.loads(response_text)
            logger.info(f"Successfully generated content for {company_name} - {product}")
            return content
        except Exception as e:
            logger.error(f"Content generation error for {company_name} - {product}: {e}")
            return None

    def generate_image(self, content, logo_id):
        company_name = content.get('company_name', 'Unknown Company')
        try:
            logger.info(f"Generating image for {company_name}")
            model = genai.GenerativeModel('gemini-1.5-flash')
            refinement_prompt = f"""
            Create a detailed image prompt for marketing:
            Punchline: {content.get('Punchline', 'Marketing Image')}
            Ensure a professional, visually appealing marketing image.
            """
            refined_completion = model.generate_content([refinement_prompt])
            refined_prompt = refined_completion.text.strip()

            # Try FLUX first
            logger.info(f"Attempting image generation with FLUX model for {company_name}")
            response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_FLUX)
            
            # If FLUX fails, try Midjourney
            if response is None:
                logger.info(f"FLUX model failed, trying Midjourney for {company_name}")
                response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_MIDJOURNEY)
                if response is None:
                    logger.error(f"Both FLUX and Midjourney failed to generate image for {company_name}")
                    return None

            if response is not None:
                image = Image.open(io.BytesIO(response.content))

                if logo_id:
                    try:
                        logger.info(f"Adding logo to image for {company_name}")
                        fs = GridFS(self.db)
                        logo_file = fs.get(logo_id)
                        logo = Image.open(io.BytesIO(logo_file.read())).convert("RGBA")
                        logo_size = (200, 100)
                        logo = logo.resize(logo_size)
                        image_width, image_height = image.size
                        logo_position = (image_width - logo_size[0] - 10, 10)
                        image_with_alpha = image.convert("RGBA")
                        image_with_alpha.paste(logo, logo_position, logo)
                        final_image = image_with_alpha.convert("RGB")
                    except Exception as logo_err:
                        logger.warning(f"Error processing logo for {company_name}: {logo_err}")
                        final_image = image
                else:
                    final_image = image

                # Save to GridFS
                img_byte_arr = io.BytesIO()
                final_image.save(img_byte_arr, format='JPEG', quality=90)
                img_byte_arr.seek(0)

                fs = GridFS(self.db)
                image_id = fs.put(
                    img_byte_arr.getvalue(),
                    filename=f"{company_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg",
                    content_type='image/jpeg'
                )

                # Save to filesystem
                company_dir = os.path.join('images', str(company_name).lower().replace(' ', '_'))
                os.makedirs(company_dir, exist_ok=True)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                image_filename = f"{company_name}_{timestamp}.jpg"
                image_path = os.path.join(company_dir, image_filename)
                final_image.save(image_path, quality=90)

                logger.info(f"Successfully saved image for {company_name} with GridFS ID: {image_id}")
                return {'image_path': image_path, 'image_id': image_id}
            else:
                logger.error(f"Failed to generate image for {company_name}")
                return None
        except Exception as e:
            logger.error(f"Image generation error for {company_name}: {e}")
            return None

    def run_marketing_content_pipeline(self):
        logger.info("Starting marketing content pipeline")
        try:
            # Fetch all companies from the database
            companies_cursor = self.db[COMPANY_COLLECTION].find()
            companies = list(companies_cursor)
            
            if not companies:
                logger.warning("No companies found in the database")
                return

            logger.info(f"Processing content generation for {len(companies)} companies")
            current_date = datetime.now()

            for company_doc in companies:
                try:
                    # Parse and validate company data
                    company = self.parse_company_data(company_doc)
                    if not company:
                        continue

                    company_name = company['company_name']
                    logger.info(f"\n{'='*50}\nProcessing company: {company_name}\n{'='*50}")

                    # Get posting schedule
                    posting_schedule = company['posting_schedule']
                    schedule_type = posting_schedule.get('type', 'daily')
                    posting_days = posting_schedule.get('days', [])

                    logger.info(f"Posting Schedule: {schedule_type}")
                    if schedule_type == 'specific_days':
                        logger.info(f"Posting Days: {', '.join(posting_days)}")

                    total_days = company['days']
                    logger.info(f"Requested content generation for {total_days} days")

                    last_content_date = self.get_last_content_date(company_name)
                    if not self.needs_content_generation(company_name, last_content_date, current_date):
                        continue

                    start_date = current_date
                    products = company['products_or_services']
                    if not products:
                        logger.warning(f"No products or services found for {company_name}")
                        continue

                    company_collection_name = f"marketing_content_{company_name.lower().replace(' ', '_')}"
                    platforms = company['target_platform']
                    if not platforms:
                        logger.warning(f"No target platforms found for {company_name}, skipping.")
                        continue

                    logo_id = company['logo_id']

                    logger.info(f"Starting content generation for {company_name}:")
                    logger.info(f"- Days to generate: {total_days}")
                    logger.info(f"- Start date: {start_date.strftime('%Y-%m-%d')}")
                    logger.info(f"- Platforms: {', '.join(platforms)}")
                    logger.info(f"- Products: {', '.join(products)}")

                    content_days_generated = 0
                    day = 0
                    product_index = 0

                    # Continue until we generate content for required number of posting days
                    while content_days_generated < total_days:
                        content_date = start_date + timedelta(days=day)
                        content_date.date()
                        print(content_date)
                        
                        # Check if we should generate content for this date
                        if not self.should_generate_content(posting_schedule, content_date):
                            day += 1
                            continue

                        for platform in platforms:
                            try:
                                product = products[product_index % len(products)]
                                
                                logger.info(f"\nGenerating content for:")
                                logger.info(f"- Company: {company_name}")
                                logger.info(f"- Platform: {platform}")
                                logger.info(f"- Date: {content_date.strftime('%Y-%m-%d')} ({content_date.strftime('%A')})")
                                logger.info(f"- Product: {product}")

                                # Check for existing content
                                existing_content = self.db[company_collection_name].find_one({
                                    'company': company_name,
                                    'platform': platform,
                                    'content_date': content_date
                                })
                                
                                if existing_content:
                                    logger.info(f"Content already exists for {company_name} on {content_date.strftime('%Y-%m-%d')} for {platform}. Skipping.")
                                    continue

                                content = self.generate_marketing_content(company, product)
                                if not content:
                                    logger.error(f"Failed to generate content for {company_name} - {product} on {platform}")
                                    continue

                                image_result = self.generate_image(content, logo_id)
                                if not image_result:
                                    logger.error(f"Failed to generate image for {company_name} - {product}")
                                    continue

                                marketing_content = {
                                    'company': company_name,
                                    'product': product,
                                    'content': content,
                                    'platform': platform,
                                    'image_path': image_result['image_path'],
                                    'image_id': image_result['image_id'],
                                    'day': content_days_generated + 1,
                                    'content_date': content_date,
                                    'generated_at': datetime.now(),
                                    'day_of_week': content_date.strftime('%A')
                                }

                                self.db[company_collection_name].insert_one(marketing_content)
                                logger.info(f"Successfully generated and saved content for {company_name} - {product} for date {content_date.strftime('%Y-%m-%d')} ({content_date.strftime('%A')}) on {platform}")

                            except Exception as platform_err:
                                logger.error(f"Error generating content for {company_name} - {product} on {platform}: {platform_err}")

                        content_days_generated += 1
                        product_index += 1
                        day += 1
                        
                        logger.info(f"Generated {content_days_generated} of {total_days} required posting days")

                    logger.info(f"Completed processing for company {company_name}")

                except Exception as company_err:
                    logger.error(f"Error processing company {company_name if 'company_name' in locals() else 'Unknown'}: {company_err}")

        except Exception as e:
            logger.error(f"Pipeline execution error: {e}")
        finally:
            logger.info("Marketing content pipeline completed")

    def close_connection(self):
        """Close the MongoDB connection"""
        self.client.close()


def main():
    """Main function to run the marketing content generator"""
    generator = MarketingContentGenerator()
    try:
        generator.run_marketing_content_pipeline()
    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
    finally:
        generator.close_connection()


if __name__ == '__main__':
    main()