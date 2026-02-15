import json
import os
from typing import Dict, List


ENTITY_LABELS = [
    "SOURCE_NAME",
    "SOURCE_CITY_CODE",
    "DESTINATION_NAME",
    "DESTINATION_CITY_CODE",
    "DEPARTURE_DATE",
    "DEPARTURE_TIME",
    "ARRIVAL_TIME",
    "PICKUP_POINT",
    "DROP_POINT",
    "AC_TYPE",
    "BUS_TYPE",
    "SEAT_TYPE",
    "AMENITIES",
    "BUS_FEATURES",
    "OPERATOR",
    "DEALS",
    "COUPON_CODE",
    "ADD_ONS",
    "PRICE",
    "SEMANTIC",
]


ENTITY_VALUES: Dict[str, List[str]] = {
    "SOURCE_NAME": [
        "Bangalore", "Bengaluru", "BLR", "Bangalore City",
        "Mumbai", "Bombay", "BOM", "Mumbai City",
        "Chennai", "Madras", "MAA",
        "Hyderabad", "Hyd", "Secunderabad",
        "Delhi", "New Delhi", "NCR Delhi",
        "Pune City", "Pune Station",
        "Coimbatore City", "Kovai",
        "Trivandrum City", "Thiruvananthapuram",
        "Visakhapatnam", "Vizag City",
        "bangalore", "bengaluru", "blr", "hyd", "mumbai",
    ],
    "SOURCE_CITY_CODE": [
        "BLR", "BENGALURU", "BANGALORE",
        "MUM", "BOM", "MUMBAI",
        "DEL", "NDLS", "DELHI", "NEW DELHI",
        "CHE", "MAA", "CHENNAI", "MADRAS",
        "HYD", "HYDERABAD", "SECUNDERABAD",
        "PUN", "PUNE",
        "CCU", "KOLKATA", "CALCUTTA",
        "AMD", "AHMEDABAD",
        "JAI", "JAIPUR",
        "LKO", "LUCKNOW",
        "COK", "KOCHI", "COCHIN",
        "TRV", "TRIVANDRUM", "THIRUVANANTHAPURAM",
        "CBE", "COIMBATORE", "KOVAI",
        "VIZ", "VTZ", "VISAKHAPATNAM", "VIZAG",
        "GOI", "GOA", "PANAJI",
        "NAG", "NAGPUR",
        "IDR", "INDORE",
        "BHO", "BHOPAL",
        "CHD", "CHANDIGARH",
        "MYS", "MYSORE",
    ],
    "DESTINATION_CITY_CODE": [
        "BLR", "BENGALURU", "BANGALORE",
        "MUM", "BOM", "MUMBAI",
        "DEL", "NDLS", "DELHI", "NEW DELHI",
        "CHE", "MAA", "CHENNAI", "MADRAS",
        "HYD", "HYDERABAD", "SECUNDERABAD",
        "PUN", "PUNE",
        "CCU", "KOLKATA", "CALCUTTA",
        "AMD", "AHMEDABAD",
        "JAI", "JAIPUR",
        "LKO", "LUCKNOW",
        "COK", "KOCHI", "COCHIN",
        "TRV", "TRIVANDRUM", "THIRUVANANTHAPURAM",
        "CBE", "COIMBATORE", "KOVAI",
        "VIZ", "VTZ", "VISAKHAPATNAM", "VIZAG",
        "GOI", "GOA", "PANAJI",
        "NAG", "NAGPUR",
        "IDR", "INDORE",
        "BHO", "BHOPAL",
        "CHD", "CHANDIGARH",
        "MYS", "MYSORE",
    ],
    "DESTINATION_NAME": [
        "Bangalore", "Bengaluru", "Mumbai", "Chennai", "Hyderabad", "Delhi",
        "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow",
        "Mysore", "Mangalore", "Coimbatore", "Kochi", "Vizag",
        "Nagpur", "Indore", "Bhopal", "Chandigarh", "Goa",
        "bangalore", "bengaluru", "mumbai", "chennai", "hyderabad",
    ],
    "DEPARTURE_DATE": [
        "today", "tomorrow", "day after tomorrow",
        "tonight", "this weekend", "next weekend",
        "monday", "tuesday", "wednesday", "thursday",
        "friday", "saturday", "sunday",
        "10 feb", "12 february", "1st march", "15/02", "2026-02-15",
    ],
    "DEPARTURE_TIME": [
        "early morning", "before sunrise",
        "morning hours", "post breakfast",
        "afternoon time", "after lunch",
        "evening hours", "sunset time",
        "night bus", "late night",
        "post midnight", "after 12 am",
        "before 6 am", "between 10 pm and 12 am",
        "around 9 pm", "approx 11 pm",
        "before 6am", "after 10pm",
        "6 am", "7am", "9:30 pm", "22:00",
    ],
    "ARRIVAL_TIME": [
        "by morning", "before noon", "after lunch",
        "early arrival", "late arrival",
        "reach by 6am", "arrive before 10",
    ],
    "PICKUP_POINT": [
        "Majestic", "Silk Board", "Electronic City",
        "BTM", "HSR", "Whitefield",
        "Gachibowli", "Kukatpally", "Ameerpet",
        "Andheri", "Borivali", "Dadar",
        "majestic", "silk board", "electronic city",
        "Silk Board Junction Bangalore",
        "Near Silk Board Bus Stop",
        "Opposite Silk Board Signal",
        "Electronic City Phase 1 Toll Gate",
        "Electronic City Phase Two Bus Stop",
        "Majestic Kempegowda Bus Station",
        "KR Puram Railway Station Main Road",
        "Hebbal Flyover Below Bridge",
        "Yelahanka New Town Circle",
        "Marathahalli Bridge Near Mall",
        "Gachibowli Flyover Below ORR",
        "Ameerpet Metro Station Gate 3",
        "LB Nagar Cross Road Bus Stop",
        "Borivali East Western Express Highway",
        "Andheri East Metro Station Gate No 2",
        "Bandra Kurla Complex Bus Bay",
        "Panvel McDonalds Highway Point",
        "Vytilla Mobility Hub Main Entrance",
        "Aluva Metro Station Parking Area",
    ],
    "DROP_POINT": [
        "Borivali West Bus Depot",
        "Near Andheri West Metro Station",
        "Thane Teen Hath Naka Circle",
        "Panvel Railway Station East",
        "Chembur Diamond Garden Signal",
        "Yeshwanthpur Railway Station Back Gate",
        "BTM Layout Second Stage",
        "HSR Layout Sector Seven Bus Stop",
        "Koyambedu Omni Bus Stand Chennai",
        "Guindy Industrial Estate Main Road",
    ],
    "AC_TYPE": [
        "AC", "Non AC", "non-ac",
        "air conditioned", "airconditioned coach",
        "with ac", "without ac",
        "no ac bus",
        "full ac sleeper",
        "non ac seater",
    ],
    "BUS_TYPE": [
      "Volvo", "VOLVO",
    "Bharat Benz", "bharat benz", "benz",
    "Electric", "electric bus",
    "Scania", "Ashok Leyland",
    "Mini bus", "Luxury bus",
    ],
    "SEAT_TYPE": [
        "Sleeper", "sleeper",
        "Seater", "seater",
        "Single", "single seat",
        "Double", "double berth",
        "Semi Sleeper", "semi sleeper", "semisleeper",
        "upper berth", "lower berth", "single sleeper lower berth",
        "double sleeper sharing",
        "semi sleeper push back seat",
        "recliner seater seat",
        "window side sleeper berth",
        "ladies seat",
        "last row sleeper",
    ],
    "AMENITIES": [
        "wifi", "wi-fi", "internet",
        "water", "water bottle",
        "blanket", "blankets",
        "bedsheet", "bedsheets",
        "pillow", "pillows",
        "charging point", "mobile charging",
        "tv", "entertainment",
    ],
    "OPERATOR": [
        "Zingbus", "zingbus",
        "Hans Travels", "hans travels",
        "KSRTC", "ksrtc",
        "MSRTC", "msrtc",
        "MHRTC", "mhrtc",
        "VRL", "SRS", "KPN",
        "Orange Travels", "Kallada Travels",
    ],
    "BUS_FEATURES": [
        "PRIMO", "primo",
        "live tracking", "gps tracking",
        "top rated", "highly rated",
        "best rated", "4 star bus", "5 star bus",
    ],
    "DEALS": [
        "MY FREE TICKET",
        "MY_DEAL", "my deal",
        "GROUP_MY_DEAL", "group deal",
        "PINK_DEAL", "pink deal", "women deal",
        "RETURN_TRIP_DEAL", "return deal", "festival season discount",
    ],
    "COUPON_CODE": [
        "WELCOMEMMT", "WELCOMEBACK", "BUS250", "BUS200", "BUS100",
        "FIRST", "FIRSTBUS", "NEWYEAR", "FESTIVE",
        "TKM", "VOMTRAIN3P482G08W",
        "MMTSUPER", "MMTBUSSUPER", "BUSSUPER",
    ],
    "ADD_ONS": [
        "Cancellation"
        "trip assured",
        "insurance", "travel insurance",
        "free cancellation",
        "cancel anytime",
        "refund available", "flexible reschedule option", "free reschedule",
    ],
    "PRICE": [
        "200", "500", "1000", "1500", "2000", "3000", "5000",
        "under 500", "under 1000", "under 2000",
        "below 3000",
        "above 2000", "above 5000",
        "cheap", "budget", "low price", "expensive",
    ],
    "SEMANTIC": [
        "fastest", "slowest",
        "not late", "on time",
        "cheapest", "lowest price",
        "best bus", "recommended",
        "comfortable", "luxury",
        "safe for women", "best rated sleeper coach",
        "most comfortable night bus",
        "safe bus option for women",
        "reliable government bus",
    ],
}


class EntityValueRegistry:
    # Generic words that must NEVER be added as entity values
    BLOCKED_VALUES = {
        "BUS_TYPE": {
            "bus", "buses", "a bus", "the bus", "bus journey",
            "bus transportation", "bus ticket", "bus tickets",
            "bus booking", "bus service", "bus route", "bus travel",
            "bus options", "bus availability", "bus schedule",
            "bus timings", "bus operator", "bus stop",
        },
        "COUPON_CODE": {
            "coupon code", "promo code", "discount code", "code",
            "coupon", "coupons", "promo", "promos", "discount", "discounts",
        },
        "DEALS": {
            "deal", "deals", "discount", "discounts", "offer", "offers",
            "coupon", "coupons", "promo", "promos",
        },
        "OPERATOR": {
            "operator", "the operator", "bus operator",
        },
        "PRICE": {
            "price", "fare", "cost", "amount", "charge", "fee",
            "charges", "fees", "pricing", "rate", "rates",
        },
        "PICKUP_POINT": {
            "pickup point", "boarding point", "pickup", "boarding",
        },
        "DROP_POINT": {
            "drop point", "dropping point", "drop", "dropping",
        },
        "ADD_ONS": {
            "add-on", "add on", "addon",
        },
        "AMENITIES": {
            "amenity", "amenities", "facility", "facilities",
        },
        "SEMANTIC": {
            "search", "find", "show", "book",
        },
    }

    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self._values = {k: list(v) for k, v in ENTITY_VALUES.items()}
        self._lower_sets = {k: {val.lower() for val in v} for k, v in self._values.items()}
        self._load_existing()

    def _load_existing(self) -> None:
        if not os.path.exists(self.storage_path):
            return
        try:
            with open(self.storage_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        if not isinstance(data, dict):
            return
        for label, values in data.items():
            if not isinstance(values, list):
                continue
            self._ensure_label(label)
            for val in values:
                if isinstance(val, str):
                    self._add_value(label, val)

    def _ensure_label(self, label: str) -> None:
        if label not in self._values:
            self._values[label] = []
            self._lower_sets[label] = set()

    def _is_blocked(self, label: str, value: str) -> bool:
        """Check if a value is on the blocklist for a given label."""
        lowered = value.lower().strip()
        blocked_set = self.BLOCKED_VALUES.get(label, set())
        return lowered in blocked_set

    def _add_value(self, label: str, value: str) -> bool:
        lowered = value.lower()
        if lowered in self._lower_sets[label]:
            return False
        if self._is_blocked(label, value):
            return False
        self._values[label].append(value)
        self._lower_sets[label].add(lowered)
        return True

    def get_reference_values(self) -> Dict[str, List[str]]:
        return {k: list(v) for k, v in self._values.items()}

    def get_entity_labels(self) -> List[str]:
        return list(ENTITY_LABELS)

    def update_with_new_values(self, new_values: Dict[str, List[str]]) -> bool:
        changed = False
        for label, values in new_values.items():
            if not isinstance(values, list):
                continue
            self._ensure_label(label)
            for val in values:
                if isinstance(val, str):
                    if self._add_value(label, val):
                        changed = True
        if changed:
            self._persist()
        return changed

    def _persist(self) -> None:
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(self._values, f, ensure_ascii=False, indent=2)
