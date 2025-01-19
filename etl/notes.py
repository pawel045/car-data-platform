import requests
from bs4 import BeautifulSoup

def scrape_otomoto_porsche_cayenne(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    # Send a GET request to the URL
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to fetch page: {response.status_code}")
        return
    
    # Parse the HTML content using BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # with open('file.txt', 'w') as file:
    #     file.write(str(soup))
    
    # Find all car listings
    listings = soup.find_all('article', class_='ooa-1rp9fi3')
    
    # Check if listings were found
    if not listings:
        print("No car listings found. The website structure may have changed.")
        return
    
    # Extract details from each listing
    cars = []
    for listing in listings:
        try:
            # Title of the car
            title = listing.find('h2', class_='ooa-1whww9o').text.strip()
            
            # Price
            price_element = listing.find('span', class_='ooa-1bmnxg7')
            price = price_element.text.strip() if price_element else "N/A"
            
            # Link to the car details
            link_element = listing.find('a', href=True)
            link = link_element['href'] if link_element else "N/A"
            
            cars.append({
                "title": title,
                "price": price,
                "link": link
            })
        except Exception as e:
            print(f"Error parsing listing: {e}")
            continue
    
    # Print the results
    print("Scraped Cars:")
    for car in cars:
        print(f"Title: {car['title']}, Price: {car['price']}, Link: {car['link']}")

# URL of the page to scrape
url = "https://www.otomoto.pl/osobowe/porsche/cayenne"

# Call the scraping function
scrape_otomoto_porsche_cayenne(url)
