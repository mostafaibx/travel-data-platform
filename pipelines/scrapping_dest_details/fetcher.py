import requests
import logging
import re
import time
from typing import Dict, Any, Optional, List, Tuple
from bs4 import BeautifulSoup
from .config import WIKIPEDIA_BASE_URL

# Configure logging
logger = logging.getLogger(__name__)


class WikipediaScraper:
    """Class for scraping destination information from Wikipedia"""

    def __init__(self, rate_limit_delay: float = 1.0):
        """
        Initialize the Wikipedia scraper

        Args:
            rate_limit_delay: Delay between requests in seconds to avoid rate limiting
        """
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Travel Data Platform/1.0 (https://github.com/yourusername/travel-data-platform; info@example.com)"
            }
        )
        self.rate_limit_delay = rate_limit_delay

    def fetch_page(
        self, destination: str
    ) -> Tuple[Optional[BeautifulSoup], Optional[Dict[str, Any]]]:
        """
        Fetch Wikipedia page for a destination

        Args:
            destination: Name of the destination

        Returns:
            Tuple containing:
            - BeautifulSoup object for parsed HTML
            - Raw response information for archiving
        """
        url = f"{WIKIPEDIA_BASE_URL}{destination.replace(' ', '_')}"

        try:
            # Add delay to respect Wikipedia's servers
            time.sleep(self.rate_limit_delay)

            response = self.session.get(url)
            response.raise_for_status()

            # Store raw response data
            raw_data = {
                "url": url,
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "content": response.text,
                "encoding": response.encoding,
            }

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(response.text, "html.parser")
            return soup, raw_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Wikipedia page for {destination}: {e}")
            return None, None

    def extract_destination_info(
        self, soup: BeautifulSoup, destination: str
    ) -> Dict[str, Any]:
        """
        Extract relevant information about a destination from Wikipedia

        Args:
            soup: BeautifulSoup object of the Wikipedia page
            destination: Name of the destination

        Returns:
            Dictionary containing structured destination information
        """
        info = {
            "destination_name": destination,
            "description": self._extract_description(soup),
            "coordinates": self._extract_coordinates(soup),
            "country": self._extract_country(soup),
            "population": self._extract_population(soup),
            "timezone": self._extract_timezone(soup),
            "languages": self._extract_languages(soup),
            "climate": self._extract_climate(soup),
            "image_url": self._extract_main_image(soup),
            "sections": self._extract_section_titles(soup),
            "attractions": self._extract_attractions(soup),
        }

        return info

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract the first paragraph description"""
        try:
            # Find the first paragraph after the intro div
            first_paragraph = soup.select_one(
                "#mw-content-text > div > p:not(.mw-empty-elt)"
            )
            if first_paragraph:
                # Clean up text by removing citations and brackets
                text = first_paragraph.text
                text = re.sub(
                    r"\[\d+\]", "", text
                )  # Remove citation numbers [1], [2], etc.
                return text.strip()
        except Exception as e:
            logger.warning(f"Error extracting description: {e}")
        return ""

    def _extract_coordinates(self, soup: BeautifulSoup) -> Dict[str, float]:
        """Extract geographic coordinates"""
        coords = {"latitude": None, "longitude": None}
        try:
            geo_tag = soup.select_one(".geo")
            if geo_tag:
                coord_text = geo_tag.text
                lat, lon = coord_text.split(";")
                coords["latitude"] = float(lat.strip())
                coords["longitude"] = float(lon.strip())
        except Exception as e:
            logger.warning(f"Error extracting coordinates: {e}")
        return coords

    def _extract_country(self, soup: BeautifulSoup) -> str:
        """Extract the country of the destination"""
        try:
            infobox = soup.select_one(".infobox")
            if infobox:
                # Look for country or location information in the infobox
                country_row = infobox.find(
                    lambda tag: tag.name == "th"
                    and ("Country" in tag.text or "Location" in tag.text)
                )
                if country_row and country_row.find_next("td"):
                    return country_row.find_next("td").text.strip()
        except Exception as e:
            logger.warning(f"Error extracting country: {e}")
        return ""

    def _extract_population(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract population information"""
        population_info = {"count": None, "year": None, "density": None}
        try:
            infobox = soup.select_one(".infobox")
            if infobox:
                # Look for population information
                pop_header = infobox.find(
                    lambda tag: tag.name == "th" and "Population" in tag.text
                )
                if pop_header:
                    # Find the closest population count
                    pop_row = pop_header.find_next("tr")
                    if pop_row and pop_row.find("td"):
                        pop_text = pop_row.find("td").text.strip()
                        # Extract the number using regex
                        pop_match = re.search(r"([\d,]+)", pop_text)
                        if pop_match:
                            population_info["count"] = int(
                                pop_match.group(1).replace(",", "")
                            )

                        # Try to extract the year
                        year_match = re.search(r"\((\d{4})\)", pop_text)
                        if year_match:
                            population_info["year"] = int(year_match.group(1))
        except Exception as e:
            logger.warning(f"Error extracting population: {e}")
        return population_info

    def _extract_timezone(self, soup: BeautifulSoup) -> str:
        """Extract timezone information"""
        try:
            infobox = soup.select_one(".infobox")
            if infobox:
                timezone_row = infobox.find(
                    lambda tag: tag.name == "th" and "Time zone" in tag.text
                )
                if timezone_row and timezone_row.find_next("td"):
                    return timezone_row.find_next("td").text.strip()
        except Exception as e:
            logger.warning(f"Error extracting timezone: {e}")
        return ""

    def _extract_languages(self, soup: BeautifulSoup) -> List[str]:
        """Extract languages spoken"""
        languages = []
        try:
            infobox = soup.select_one(".infobox")
            if infobox:
                lang_row = infobox.find(
                    lambda tag: tag.name == "th"
                    and (
                        "Language" in tag.text
                        or "Languages" in tag.text
                        or "Official language" in tag.text
                    )
                )
                if lang_row and lang_row.find_next("td"):
                    lang_text = lang_row.find_next("td").text.strip()
                    # Split and clean language list
                    languages = [
                        lang.strip()
                        for lang in re.split(r",|\n", lang_text)
                        if lang.strip()
                    ]
        except Exception as e:
            logger.warning(f"Error extracting languages: {e}")
        return languages

    def _extract_climate(self, soup: BeautifulSoup) -> str:
        """Extract climate information"""
        try:
            climate_section = soup.find(
                lambda tag: tag.name == "span"
                and tag.get("id")
                in ["Climate", "Geography_and_climate", "Weather", "Environment"]
            )
            if climate_section:
                section_header = climate_section.parent
                paragraph = section_header.find_next("p")
                if paragraph:
                    # Clean up text
                    text = paragraph.text
                    text = re.sub(r"\[\d+\]", "", text)  # Remove citation numbers
                    return text.strip()
        except Exception as e:
            logger.warning(f"Error extracting climate info: {e}")
        return ""

    def _extract_main_image(self, soup: BeautifulSoup) -> str:
        """Extract URL of the main image"""
        try:
            infobox = soup.select_one(".infobox")
            if infobox:
                image_tag = infobox.select_one(".image img")
                if image_tag and "src" in image_tag.attrs:
                    # Convert relative URL to absolute URL if needed
                    src = image_tag["src"]
                    if src.startswith("//"):
                        return f"https:{src}"
                    return src
        except Exception as e:
            logger.warning(f"Error extracting main image: {e}")
        return ""

    def _extract_section_titles(self, soup: BeautifulSoup) -> List[str]:
        """Extract main section titles from the page"""
        sections = []
        try:
            for heading in soup.find_all(["h2", "h3"]):
                if heading.find("span", class_="mw-headline"):
                    section_title = heading.find(
                        "span", class_="mw-headline"
                    ).text.strip()
                    if section_title not in [
                        "References",
                        "External links",
                        "See also",
                        "Notes",
                    ]:
                        sections.append(section_title)
        except Exception as e:
            logger.warning(f"Error extracting section titles: {e}")
        return sections

    def _extract_attractions(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract tourist attractions from the page"""
        attractions = []
        try:
            # Look for common section titles that contain attraction information
            attraction_section_ids = [
                "Attractions",
                "Tourism",
                "Landmarks",
                "Sights",
                "Tourist_attractions",
                "Places_of_interest",
                "Main_sights",
                "Notable_attractions",
                "Points_of_interest",
            ]

            # First try to find sections dedicated to attractions
            for section_id in attraction_section_ids:
                section = soup.find(
                    lambda tag: tag.name == "span"
                    and tag.get("id") == section_id
                    or (
                        tag.get("class")
                        and "mw-headline" in tag.get("class")
                        and section_id.replace("_", " ") in tag.text
                    )
                )

                if section:
                    # Found a section with attractions, look for lists
                    section_header = section.parent
                    attractions.extend(self._parse_attraction_list(section_header))

            # If no dedicated section found, try looking for lists throughout the article
            if not attractions:
                # Check if there's any "must-see" list in the page
                for list_elem in soup.find_all(["ul", "ol"]):
                    # Skip if in irrelevant sections
                    parent_section = list_elem.find_previous(["h2", "h3"])
                    if parent_section and parent_section.find(
                        "span", class_="mw-headline"
                    ):
                        section_title = parent_section.find(
                            "span", class_="mw-headline"
                        ).text.strip()
                        if section_title in [
                            "References",
                            "External links",
                            "See also",
                            "Notes",
                            "Bibliography",
                        ]:
                            continue

                    # Check if list contains attractions (look for specific keywords)
                    list_text = list_elem.text.lower()
                    attraction_keywords = [
                        "museum",
                        "monument",
                        "park",
                        "palace",
                        "castle",
                        "cathedral",
                        "temple",
                        "landmark",
                        "attraction",
                        "garden",
                        "square",
                        "tower",
                    ]

                    if (
                        any(keyword in list_text for keyword in attraction_keywords)
                        and len(list_elem.find_all("li")) > 1
                    ):
                        attractions.extend(
                            self._parse_attraction_list_element(list_elem)
                        )

            # Deduplicate attractions based on name
            unique_attractions = []
            seen_names = set()
            for attraction in attractions:
                if attraction["name"].lower() not in seen_names:
                    seen_names.add(attraction["name"].lower())
                    unique_attractions.append(attraction)

            return unique_attractions

        except Exception as e:
            logger.warning(f"Error extracting attractions: {e}")

        return attractions

    def _parse_attraction_list(self, section_header) -> List[Dict[str, str]]:
        """Parse attraction list from a section"""
        attractions = []

        # Find the next list elements after the section header
        next_elem = section_header.next_sibling
        while next_elem:
            if next_elem.name in ["ul", "ol"]:
                attractions.extend(self._parse_attraction_list_element(next_elem))
            elif next_elem.name in ["h2", "h3", "h4"]:
                # Stop if we hit another section header
                break
            elif next_elem.name == "p" and len(next_elem.text.strip()) > 50:
                # Extract attraction from paragraph if it's substantial
                description = next_elem.text.strip()
                description = re.sub(
                    r"\[\d+\]", "", description
                )  # Remove citation numbers

                # Try to extract attraction names from bold text
                bold_tags = next_elem.find_all("b")
                if bold_tags:
                    for bold in bold_tags:
                        name = bold.text.strip()
                        if name and len(name) > 3 and name not in ["edit"]:
                            attractions.append(
                                {
                                    "name": name,
                                    "description": description,
                                    "image_url": "",
                                    "type": self._guess_attraction_type(name),
                                }
                            )

            next_elem = next_elem.next_sibling

        return attractions

    def _parse_attraction_list_element(self, list_elem) -> List[Dict[str, str]]:
        """Parse attractions from a list element"""
        attractions = []

        # Find all list items
        for item in list_elem.find_all("li", recursive=False):
            # Extract name (usually the first link or bold text)
            name = ""
            link = item.find("a")
            bold = item.find("b")

            if bold:
                name = bold.text.strip()
            elif link:
                name = link.text.strip()
            else:
                # Try to get the first part of the text as name
                item_text = item.text.strip()
                colon_pos = item_text.find(":")
                dash_pos = item_text.find(" - ")

                if colon_pos > 0:
                    name = item_text[:colon_pos].strip()
                elif dash_pos > 0:
                    name = item_text[:dash_pos].strip()
                else:
                    # Just take the first part if it's not too long
                    words = item_text.split()
                    if len(words) <= 5:
                        name = item_text
                    else:
                        # Take first 3-5 words as name
                        name = " ".join(words[: min(5, max(3, len(words) // 3))])

            # Clean up the name
            name = re.sub(r"\[\d+\]", "", name)  # Remove citation numbers

            if name and len(name) > 3:
                # Extract description (rest of the list item text)
                description = item.text.replace(name, "", 1).strip()
                description = re.sub(
                    r"\[\d+\]", "", description
                )  # Remove citation numbers
                description = description.lstrip(":-–— ")  # Remove leading punctuation

                # Extract image if available
                image_url = ""
                img = item.find("img")
                if img and "src" in img.attrs:
                    src = img["src"]
                    if src.startswith("//"):
                        image_url = f"https:{src}"
                    else:
                        image_url = src

                # Guess attraction type
                attraction_type = self._guess_attraction_type(name)

                attractions.append(
                    {
                        "name": name,
                        "description": description,
                        "image_url": image_url,
                        "type": attraction_type,
                    }
                )

        return attractions

    def _guess_attraction_type(self, name: str) -> str:
        """Guess the type of attraction based on its name"""
        name_lower = name.lower()

        if any(
            keyword in name_lower for keyword in ["museum", "gallery", "exhibition"]
        ):
            return "museum"
        elif any(
            keyword in name_lower
            for keyword in ["palace", "castle", "mansion", "estate"]
        ):
            return "palace"
        elif any(keyword in name_lower for keyword in ["park", "garden", "botanical"]):
            return "park"
        elif any(
            keyword in name_lower
            for keyword in [
                "cathedral",
                "church",
                "temple",
                "mosque",
                "shrine",
                "chapel",
            ]
        ):
            return "religious"
        elif any(
            keyword in name_lower for keyword in ["monument", "memorial", "statue"]
        ):
            return "monument"
        elif any(
            keyword in name_lower
            for keyword in ["tower", "skyscraper", "building", "center", "centre"]
        ):
            return "building"
        elif any(keyword in name_lower for keyword in ["square", "plaza", "piazza"]):
            return "square"
        elif any(keyword in name_lower for keyword in ["theater", "theatre", "opera"]):
            return "theatre"
        elif any(keyword in name_lower for keyword in ["bridge", "tunnel"]):
            return "bridge"
        elif any(keyword in name_lower for keyword in ["zoo", "aquarium"]):
            return "zoo"
        elif any(keyword in name_lower for keyword in ["beach", "coast", "shore"]):
            return "beach"
        elif any(keyword in name_lower for keyword in ["market", "bazaar", "shopping"]):
            return "market"
        else:
            return "landmark"


# Function to get destination info
def get_destination_info(
    destination: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Get information about a travel destination from Wikipedia

    Args:
        destination: Name of the destination

    Returns:
        Tuple containing:
        - Structured destination information
        - Raw Wikipedia response data
    """
    scraper = WikipediaScraper()
    soup, raw_data = scraper.fetch_page(destination)

    if soup is None:
        logger.error(f"Failed to fetch Wikipedia page for {destination}")
        return None, None

    try:
        destination_info = scraper.extract_destination_info(soup, destination)
        return destination_info, raw_data
    except Exception as e:
        logger.error(f"Error extracting destination info for {destination}: {e}")
        return None, raw_data
