from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import sqlite3
from sqlite3 import Error
from haversine import haversine, Unit

# Database setup
def create_connection(db_file):
    """ create a database connection to the SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

# Model for the address data
class Address(BaseModel):
    id: int
    street: str
    city: str
    state: str
    country: str
    latitude: float
    longitude: float

# FastAPI app initialization
app = FastAPI()

# In-memory SQLite database
DATABASE_URL = "sqlite:///./test.db"

# Create addresses table
create_addresses_table_sql = """
CREATE TABLE IF NOT EXISTS addresses (
    id integer PRIMARY KEY,
    street text NOT NULL,
    city text NOT NULL,
    state text NOT NULL,
    country text NOT NULL,
    latitude real NOT NULL,
    longitude real NOT NULL
);
"""

# Establish a database connection and create the table
conn = create_connection(DATABASE_URL)
if conn is not None:
    create_table(conn, create_addresses_table_sql)
else:
    print("Error! cannot create the database connection.")

# CRUD operations for the address data
@app.post("/addresses/", response_model=Address)
def create_address(address: Address, conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Create a new address in the database.
    """
    sql = ''' INSERT INTO addresses(street,city,state,country,latitude,longitude)
              VALUES(?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, (address.street, address.city, address.state, address.country, address.latitude, address.longitude))
    conn.commit()
    return address

@app.get("/addresses/", response_model=List[Address])
def read_addresses(conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Retrieve all addresses from the database.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM addresses")
    rows = cur.fetchall()
    return [Address(**row) for row in rows]

@app.get("/addresses/{address_id}", response_model=Address)
def read_address(address_id: int, conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Retrieve a single address by id from the database.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM addresses WHERE id=?", (address_id,))
    row = cur.fetchone()
    if row:
        return Address(**row)
    raise HTTPException(status_code=404, detail="Address not found")

@app.put("/addresses/{address_id}", response_model=Address)
def update_address(address_id: int, address: Address, conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Update an address in the database.
    """
    sql = ''' UPDATE addresses
              SET street=?, city=?, state=?, country=?, latitude=?, longitude=?
              WHERE id=? '''
    cur = conn.cursor()
    cur.execute(sql, (address.street, address.city, address.state, address.country, address.latitude, address.longitude, address_id))
    conn.commit()
    return address

@app.delete("/addresses/{address_id}")
def delete_address(address_id: int, conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Delete an address from the database.
    """
    sql = 'DELETE FROM addresses WHERE id=?'
    cur = conn.cursor()
    cur.execute(sql, (address_id,))
    conn.commit()
    return {"message": "Address with id {} deleted".format(address_id)}


@app.get("/addresses/distance/")
def read_addresses_within_distance(latitude: float, longitude: float, max_distance: float, conn: sqlite3.Connection = create_connection(DATABASE_URL)):
    """
    Retrieve addresses within a given distance from the provided coordinates.
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM addresses")
    rows = cur.fetchall()

    # Convert the provided latitude and longitude into a tuple
    origin = (latitude, longitude)

    # List to hold addresses within the specified max_distance
    nearby_addresses = []

    for row in rows:
        # Convert the address's latitude and longitude into a tuple
        address_coords = (row['latitude'], row['longitude'])

        # Calculate the distance between the origin and the address
        distance = haversine(origin, address_coords, unit=Unit.KILOMETERS)

        # If the address is within the max_distance, add it to the list
        if distance <= max_distance:
            nearby_addresses.append(Address(**row))

    return nearby_addresses
