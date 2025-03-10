import os
import json
import base64
import logging
from datetime import datetime
import requests
import pymongo
import google.generativeai as genai
from PIL import Image
import io
from db_connection import get_mongo_client
import random
from gridfs import GridFS

logging.getLogger("pymongo").setLevel(logging.info)
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

    def query_huggingface(self, payload, api_url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.post(api_url, headers=HF_HEADERS, json=payload)
                if response.status_code == 200:
                    return response
                if response.status_code == 503:
                    logger.warning("Service temporarily unavailable")
                    continue
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"API call failed: {str(e)}")
                continue
        return None

    def generate_marketing_content(self, company_data, product):
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            input_prompt = f"""
            Generate unique marketing content for:
            - Company Name: {company_data.get('company_name', 'Unknown Company')}
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
            """
            # logger.debug(f"Generating content for {product}")
            completion = model.generate_content([input_prompt])
            response_text = completion.text.strip()
            if response_text.startswith('```json'):
                response_text = response_text.replace('```json', '').strip()
            if response_text.endswith('```'):
                response_text = response_text[:-3].strip()
            content = json.loads(response_text)
            logger.info(f"Successfully generated content for {product}")
            return content
        except Exception as e:
            logger.error(f"Content generation error for {product}: {e}")
            return None

    # def generate_image(self, content, logo_data):
    #     try:
    #         model = genai.GenerativeModel('gemini-1.5-flash')
    #         refinement_prompt = f"""
    #         Create a detailed image prompt for marketing:
    #         Punchline: {content.get('Punchline', 'Marketing Image')}
    #         Ensure a professional, visually appealing marketing image.
    #         """
    #         refined_completion = model.generate_content([refinement_prompt])
    #         refined_prompt = refined_completion.text.strip()
    #         response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_FLUX)
    #         if response is None:
    #             response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_MIDJOURNEY)
    #         if response is not None:
    #             image = Image.open(io.BytesIO(response.content))
    #             if logo_data:
    #                 try:
    #                     logo = Image.open(io.BytesIO(base64.b64decode(logo_data))).convert("RGBA")
    #                     logo_size = (200, 100)
    #                     logo = logo.resize(logo_size)
    #                     image_width, image_height = image.size
    #                     logo_position = (image_width - logo_size[0] - 10, 10)
    #                     image_with_alpha = image.convert("RGBA")
    #                     image_with_alpha.paste(logo, logo_position, logo)
    #                     final_image = image_with_alpha.convert("RGB")
    #                 except Exception as logo_err:
    #                     logger.warning(f"Logo processing error: {logo_err}")
    #                     final_image = image
    #             else:
    #                 final_image = image
    #             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #             image_filename = f"{content.get('Title', 'marketing_image')}_{timestamp}.jpg"
    #             image_path = os.path.join(self.images_dir, image_filename)
    #             final_image.save(image_path, quality=90)
    #             logger.info(f"Image saved: {image_path}")
    #             return image_path
    #     except Exception as e:
    #         logger.error(f"Image generation error: {e}")
    #         return None
    # def generate_image(self, content, logo_id, company_name):
    #     try:
    #         # Initialize the generative model
    #         model = genai.GenerativeModel('gemini-1.5-flash')
    #         refinement_prompt = f"""
    #         Create a detailed image prompt for marketing:
    #         Punchline: {content.get('Punchline', 'Marketing Image')}
    #         Ensure a professional, visually appealing marketing image.
    #         """
    #         refined_completion = model.generate_content([refinement_prompt])
    #         refined_prompt = refined_completion.text.strip()

    #         # Query Hugging Face for image generation
    #         response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_FLUX)
    #         if response is None:
    #             response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_MIDJOURNEY)

    #         if response is not None:
    #             image = Image.open(io.BytesIO(response.content))

    #             # Retrieve logo from GridFS if logo_id is provided
    #             if logo_id:
    #                 try:
    #                     fs = GridFS(self.db)  # Initialize GridFS with your database
    #                     logo_file = fs.get(logo_id)  # Get logo binary data by ID
    #                     logo = Image.open(io.BytesIO(logo_file.read())).convert("RGBA")
    #                     logo_size = (200, 100)
    #                     logo = logo.resize(logo_size)
    #                     image_width, image_height = image.size
    #                     logo_position = (image_width - logo_size[0] - 10, 10)
    #                     image_with_alpha = image.convert("RGBA")
    #                     image_with_alpha.paste(logo, logo_position, logo)
    #                     final_image = image_with_alpha.convert("RGB")
    #                 except Exception as logo_err:
    #                     logger.warning(f"Error retrieving or processing logo for {company_name}: {logo_err}")
    #                     final_image = image
    #             else:
    #                 final_image = image

    #             # Create a unique directory for each company
    #             company_dir = os.path.join(self.images_dir, company_name.lower().replace(' ', '_'))
    #             os.makedirs(company_dir, exist_ok=True)

    #             # Save the image with a timestamped filename
    #             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #             image_filename = f"{content.get('Title', 'marketing_image')}_{timestamp}.jpg"
    #             image_path = os.path.join(company_dir, image_filename)
    #             final_image.save(image_path, quality=90)

    #             logger.info(f"Image saved for {company_name}: {image_path}")
    #             return image_path
    #         else:
    #             logger.error(f"Failed to generate image for {company_name}")
    #             return None
    #     except Exception as e:
    #         logger.error(f"Image generation error for {company_name}: {e}")
    #     return None

    def generate_image(self, content, logo_id):
        try:
            # Initialize the generative model
            model = genai.GenerativeModel('gemini-1.5-flash')
            refinement_prompt = f"""
            Create a detailed image prompt for marketing:
            Punchline: {content.get('Punchline', 'Marketing Image')}
            Ensure a professional, visually appealing marketing image.
            """
            refined_completion = model.generate_content([refinement_prompt])
            refined_prompt = refined_completion.text.strip()

            # Query Hugging Face for image generation
            response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_FLUX)
            if response is None:
                response = self.query_huggingface({"inputs": refined_prompt}, HF_API_URL_MIDJOURNEY)

            if response is not None:
                image = Image.open(io.BytesIO(response.content))

                # Retrieve logo from GridFS if logo_id is provided
                if logo_id:
                    try:
                        fs = GridFS(self.db)  # Initialize GridFS with your database
                        logo_file = fs.get(logo_id)  # Get logo binary data by ID
                        logo = Image.open(io.BytesIO(logo_file.read())).convert("RGBA")
                        logo_size = (200, 100)
                        logo = logo.resize(logo_size)
                        image_width, image_height = image.size
                        logo_position = (image_width - logo_size[0] - 10, 10)
                        image_with_alpha = image.convert("RGBA")
                        image_with_alpha.paste(logo, logo_position, logo)
                        final_image = image_with_alpha.convert("RGB")
                    except Exception as logo_err:
                        logger.warning(f"Error retrieving or processing logo: {logo_err}")
                        final_image = image
                else:
                    final_image = image

                # Extract the company name from content
                # company_name = content.get("company_name", "default_company_name")

                # # Create a unique directory for each company
                # company_dir = os.path.join(self.images_dir, str(company_name).lower().replace(' ', '_'))
                company_name = str(content.get('company_name', 'default_company')).lower().replace(' ', '_')
                company_dir = os.path.join('images', company_name)
                os.makedirs(company_dir, exist_ok=True)

                # Save the image with a timestamped filename
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                image_filename = f"{content.get('company_name', 'platform')}_{timestamp}.jpg"
                image_path = os.path.join(company_dir, image_filename)
                final_image.save(image_path, quality=90)

                logger.info(f"Image saved for {company_name}: {image_path}")
                return image_path
            else:
                logger.error(f"Failed to generate image for {content}")
                return None
        except Exception as e:
            logger.error(f"Image generation error for {content}: {e}")
            return None

    # def run_marketing_content_pipeline(self):
    #     companies = list(self.db[COMPANY_COLLECTION].find())
    #     if not companies:
    #         logger.warning("No companies found in the database")
    #         return

    #     for company in companies:
    #         try:
    #             company_name = company.get('company_name', 'Unknown Company')
    #             products = company.get('products_or_services', [company_name])
    #             company_collection_name = f"marketing_content_{company_name.lower().replace(' ', '_')}"
    #             platform=company.get("target_platform")
    #             days=company.get('days', '1')
    #             # logger.debug(f"Processing collection: {company_collection_name}")
    #             # self.db[company_collection_name].delete_many({})

    #             for day in days:
    #                 try:
    #                     content = self.generate_marketing_content(company, product)
    #                     if not content:
    #                         logger.error(f"Failed to generate content for {company_name} - {product}")
    #                         continue
    #                     image_path = self.generate_image(content, company.get('logo', ''))
    #                     marketing_content = {
    #                         'company': company_name,
    #                         'product': product,
    #                         'content': content,
    #                         'Platform': platform,
    #                         'image_path': image_path,
    #                         'generated_at': datetime.now()
    #                     }
    #                     result = self.db[company_collection_name].insert_one(marketing_content)
    #                     logger.info(f"Generated content for {company_name} - {product}")
    #                     # logger.debug(f"Inserted document ID: {result.inserted_id}")
    #                 except Exception as product_err:
    #                     logger.error(f"Error processing product {product}: {product_err}")
    #         except Exception as company_err:
    #             logger.error(f"Error processing company {company_name}: {company_err}")


    def run_marketing_content_pipeline(self):
        companies = list(self.db[COMPANY_COLLECTION].find())
        if not companies:
            logger.warning("No companies found in the database")
            return

        for company in companies:
            try:
                company_name = company.get('company_name', 'Unknown Company')
                products = company.get('products_or_services', [company_name])
                if isinstance(products, str):  # Convert a single string to a list
                    products = [p.strip() for p in products.split(",") if p.strip()]
                if not products:
                    logger.warning(f"No products or services found for {company_name}")
                    continue

                company_collection_name = f"marketing_content_{company_name.lower().replace(' ', '_')}"
                platforms = company.get("target_platform", ["General"])  # Default to "General" if no platform provided
                if isinstance(platforms, str):  # Convert a single platform string to a list
                    platforms = [p.strip() for p in platforms.split(",") if p.strip()]
                if not platforms:
                    logger.warning(f"No target platforms found for {company_name}, skipping.")
                    continue

                total_days = int(company.get('days', 1))
                logo_path = company.get('logo_id', '')

                # Initialize the generative model
                model = genai.GenerativeModel('gemini-1.5-flash')

                for platform in platforms:  # Platform-wise loop
                    for day in range(1, total_days + 1):  # Day-wise loop
                        try:
                            # Cycle through products or randomly select a product
                            if day <= len(products):
                                product = products[day - 1]
                            else:
                                product = random.choice(products)

                            content = self.generate_marketing_content(company, product)
                            if not content:
                                logger.error(f"Failed to generate content for {company_name} - {product} on {platform}")
                                continue

                            image_path = self.generate_image(content, logo_path)
                            

                            marketing_content = {
                                'company': company_name,
                                'product': product,
                                'content': content,
                                'platform': platform,
                                'image_path': image_path,
                                'day': day,
                                'generated_at': datetime.now()
                            }

                            # Insert into the database
                            result = self.db[company_collection_name].insert_one(marketing_content)
                            logger.info(f"Generated content for {company_name} - {product} on Day {day} for {platform}")
                        except Exception as product_err:
                            logger.error(f"Error generating content for {company_name} - {product} on {platform}: {product_err}")
            except Exception as company_err:
                logger.error(f"Error processing company {company_name}: {company_err}")


    def close_connection(self):
        self.client.close()

def main():
    generator = MarketingContentGenerator()
    try:
        generator.run_marketing_content_pipeline()
    except Exception as e:
        logger.error(f"Pipeline execution error: {e}")
    finally:
        generator.close_connection()

if __name__ == '__main__':
    main()