import json
import mysql.connector

def load_json(d):
    with open(d,'r') as f:
        data = json.load(f)
    return data

conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="actowiz",
  database="vishal"
)

cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS flight_data (
    Flight_Id VARCHAR(50) UNIQUE,
    Origin VARCHAR(50),
    Destination VARCHAR(50),
    AirLine VARCHAR(50),
    Departure_Time VARCHAR(10),   
    Arrival_Time VARCHAR(10),
    Duration_Time VARCHAR(10),
    AirLine_Code VARCHAR(50),
    Price INT,                     
    CabinClass VARCHAR(25),
    Hand_Baggage VARCHAR(50),
    RefundableType VARCHAR(50),
    MealAvailable BOOLEAN,
    Rating FLOAT
);
''')

def process(d):
    flight_data = []  # List to store all the flight data
    base_path = d.get("data", {}).get("flightJourneys", [])
    for i in base_path:
        if isinstance(i, dict):
            f = i.get("flightFare", [])
            for j in f:
                e_dict = {}  # Initialize e_dict for each flight
                # Extract the flight keys & details
                e_dict["FilghtKey"] = j.get("flightKeys")  # Assuming FlightKey is unique
                o = j.get("flightDetails", [])
                # Process Origin & Destination
                for k in o:
                    e_dict["Origin"] = k.get("origin")
                    e_dict["Destination"] = k.get("destination")
                # Process Airline & Airline Code
                for r in o:
                    if isinstance(r, dict):
                        e_dict["AirLine"] = r.get("headerTextWeb")
                        e_dict["AirLineCode"] = r.get("subHeaderTextWeb")
                # Process Departure & Arrival Time
                for t in o:
                    if isinstance(t, dict):
                        e_dict["DepartureTime"] = t.get("departureTime")
                        e_dict['ArrivalTime'] = t.get("arrivalTime")
                # Process Duration
                for dt in o:
                    e_dict["Duration_Time"] = dt.get("duration", {}).get("text")
                # Process Fare & CabinClass
                fare = j.get("fares", [])
                for f in fare:
                    e_dict["Price"] = f.get("fareDetails", {}).get("displayFare")
                    e_dict["CabinClass"] = f.get("fareMetadata", [])[0].get("cabinClass")
                    e_dict["HandBaggage"] = f.get("fareMetadata", [])[0].get("handBaggageOnly")
                # Process Refund, Meal, & Rating
                e_dict["RefundableType"] = j.get("refundableType")
                e_dict["MealAvailable"] = j.get("isFreeMealAvailable")
                e_dict["Rating"] = j.get("sort", {}).get("fastestRating")

                flight_data.append(e_dict) 

    return flight_data  


def dump_file(data_extracted):
    with open("Extracted_flight.json", "wb") as f:
        f.write(json.dumps(data_extracted, indent=4).encode())

# Load and process the JSON file
base_json_file = r"C:\Users\vishal.mistry\Desktop\Mistry Vishal\flight data\ixigo_flight.json"
file_input = load_json(base_json_file)
flight_data = process(file_input)
dump_file(flight_data)

for f in flight_data:
    query = """
    INSERT INTO flight_data (
        Flight_Id, Origin, Destination, AirLine, AirLine_Code, 
        Departure_Time, Arrival_Time, Duration_Time, Price, 
        CabinClass, Hand_Baggage, RefundableType, MealAvailable, Rating
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, 
        %s, %s, %s, %s, %s
    )
    """
    values = (
        f.get("FilghtKey"), 
        f.get("Origin"), 
        f.get("Destination"), 
        f.get("AirLine"), 
        f.get("AirLineCode"),
        f.get("DepartureTime"), 
        f.get("ArrivalTime"), 
        f.get("Duration_Time"), 
        f.get("Price"), 
        f.get("CabinClass"), 
        f.get("HandBaggage"), 
        f.get("RefundableType"), 
        f.get("MealAvailable"), 
        f.get("Rating")
    )

    cursor.execute(query, values)
conn.commit()

cursor.close()
conn.close()