import time
import requests
from bs4 import BeautifulSoup
import re
import concurrent.futures
import urllib3
import boto3
import json
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime

today_date = datetime.now().strftime("%Y%m%d")

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# AWS S3 configuration
S3_BUCKET_NAME = 'cognitive-airlineops-lambda-converter'
S3_FOLDER_NAME = f'extract_data/{today_date}'  # Replace with your desired folder name
LINKS_FILE_KEY = 'WebScrapping/links_file.json'  # Updated path for links_file.json

# Initialize S3 client using default credentials
s3_client = boto3.client('s3')

def download_links_file(s3_client, bucket_name, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"File {file_key} not found in S3. Creating a new one.")
            return {"links": []}
        else:
            raise

def upload_links_file(s3_client, bucket_name, file_key, links_file):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=json.dumps(links_file),
            ContentType='application/json'
        )
        print(f"Uploaded links file to S3: s3://{bucket_name}/{file_key}")
    except ClientError as e:
        print(f"Error uploading links file to S3: {str(e)}")

def find_eaip_url():
    url = "https://aim-india.aai.aero/aip-supplements?page=1"
    response = requests.get(url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the AIP supplements page. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    for li_tag in soup.find_all('li', class_="leaf new"):
        a_tag = li_tag.find('a', href=True)
        if a_tag and a_tag.text.strip().startswith("eAIP India"):
            href = a_tag['href']
            print(f"Found eAIP link: {href}")
            return href

    print("No EAIP URL found.")
    return None

def fetch_and_parse_frameset(eaip_url):
    response = requests.get(eaip_url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the EAIP frameset. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    frameset = soup.find('frameset', attrs={'onload': 'openTarget()'})
    
    if frameset:
        frame = frameset.find('frame', {'name': 'eAISNavigationBase'})
        if frame and 'src' in frame.attrs:
            frame_url = frame['src']
            return f"{eaip_url.rsplit('/', 1)[0]}/{frame_url}"

    print("No frame found with the name 'eAISNavigationBase'.")
    return None

def fetch_and_parse_navigation_frame(base_frame_url):
    response = requests.get(base_frame_url, verify=False)

    if response.status_code != 200:
        print(f"Failed to fetch the base frame page. Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    frame = soup.find('frame', {'name': 'eAISNavigation'})
    
    if frame and 'src' in frame.attrs:
        frame_url = frame['src']
        return f"{base_frame_url.rsplit('/', 1)[0]}/{frame_url}"

    print("No frame found with the name 'eAISNavigation'.")
    return None

def upload_pdf_to_s3(pdf_url, airport_code):
    try:
        response = requests.get(pdf_url, verify=False)
        if response.status_code == 200:
            file_name = pdf_url.split("/")[-1]
            s3_key = f"{S3_FOLDER_NAME}/{airport_code}/{file_name}"
            
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=response.content,
                ContentType='application/pdf'
            )
            print(f"Uploaded to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
            return True
        else:
            print(f"Failed to download PDF from {pdf_url}. Status code: {response.status_code}")
            return False
    except ClientError as e:
        print(f"Error uploading {pdf_url} to S3: {str(e)}")
        return False

def process_airport(aerodrome, base_url):
    aerodrome_name = aerodrome.text.strip()
    aerodrome_href = aerodrome['href']
    airport_code = aerodrome['id'].split('.')[-1][1:]
    
    print(f"Processing: {aerodrome_name}: {airport_code}")
    fetch_and_print_iframe_src(aerodrome_href, base_url, airport_code)

def fetch_and_print_airports(navigation_url):
    response = requests.get(navigation_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the navigation content. Status code: {response.status_code}")
        return False

    soup = BeautifulSoup(response.text, 'html.parser')
    aerodromes = soup.find_all('a', href=True, id=re.compile(r'^AD 2\.1V[A-Z]+'))
    base_url = navigation_url.rsplit('/', 1)[0]

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_airport, aerodrome, base_url) for aerodrome in aerodromes[:2]]
        concurrent.futures.wait(futures)


    return True

def fetch_and_print_iframe_src(airport_href, base_url, airport_code):
    airport_url = f"{base_url}/{airport_href}"
    response = requests.get(airport_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the airport page. Status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    iframe = soup.find('iframe', {'id': 'IncludeFileBottom'})
    if iframe and 'src' in iframe.attrs:
        iframe_src = iframe['src']
        full_iframe_url = f"{airport_url.rsplit('/', 1)[0]}/{iframe_src}"
        print(f"Iframe source: {full_iframe_url}")
        check_iframe_content_for_pdfs(full_iframe_url, airport_code)
    else:
        print("No iframe found with the ID 'IncludeFileBottom'.")

def check_iframe_content_for_pdfs(iframe_url, airport_code):
    response = requests.get(iframe_url, verify=False)
    if response.status_code != 200:
        print(f"Failed to fetch the iframe content. Status code: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')
    pdf_links = soup.find_all('a', href=True)
    
    pdf_urls = []
    for link in pdf_links:
        pdf_url = link['href']
        if pdf_url.endswith('.pdf'):
            full_pdf_url = pdf_url if pdf_url.startswith("http") else f"{iframe_url.rsplit('/', 1)[0]}/{pdf_url}"
            pdf_urls.append((full_pdf_url, airport_code))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(upload_pdf_to_s3, url, code) for url, code in pdf_urls]
        concurrent.futures.wait(futures)

def lambda_handler(event, context):
    starttime = time.time()
    try:
        # Download links file from S3
        links_file = download_links_file(s3_client, S3_BUCKET_NAME, LINKS_FILE_KEY)
        processed_links = links_file.get("links", [])

        # Find eAIP URL
        eaip_url = find_eaip_url()
        if not eaip_url:
            print("New eAIP Link Not Yet Published")
            return {
                'statusCode': 404,
                'body': json.dumps('New eAIP Link Not Yet Published')
            }

        # Check if eAIP URL is already processed
        if eaip_url in processed_links:
            print("New eAIP Link Not Yet Published")
            return {
                'statusCode': 200,
                'body': json.dumps('New eAIP Link Not Yet Published')
            }

        # Process new eAIP
        base_frame_url = fetch_and_parse_frameset(eaip_url)
        if not base_frame_url:
            print("Failed to parse frameset")
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to parse frameset')
            }

        navigation_frame_url = fetch_and_parse_navigation_frame(base_frame_url)
        if not navigation_frame_url:
            print("Failed to parse navigation frame")
            return {
                'statusCode': 500,
                'body': json.dumps('Failed to parse navigation frame')
            }

        fetch_and_print_airports(navigation_frame_url)

        # Update and upload links file to S3
        processed_links.append(eaip_url)
        links_file["links"] = processed_links
        upload_links_file(s3_client, S3_BUCKET_NAME, LINKS_FILE_KEY, links_file)

        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed successfully')
        }
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        endtime = time.time()
        print(f"Execution time: {endtime - starttime:.2f} seconds")       

if __name__ == "__main__":
    result = lambda_handler(None, None)
    print(result)  # Print the return value
