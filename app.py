# Import the dependencies.
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///D://_Documents//_Data Analytics Material//Challenges//Module 10//Starter_Code//Resources//hawaii.sqlite")

# reflect an existing database into a new model
new_table = automap_base()

# reflect the tables
new_table.prepare(autoload_with=engine)

# Save references to each table
table_station = new_table.classes.station
table_measurements = new_table.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def main_page():
    return "What's up gamer"

@app.route("/api/v1.0/precipitation")
def prcp():
    session = Session(engine)
    ### This chunk is from code I've already written
    latest_date_query = session.query(func.max(table_measurements.date)).scalar()

    latest_date = datetime.strptime(latest_date_query, '%Y-%m-%d')
    start_date = latest_date - timedelta(days=365)

    range_of_dates = session.query(table_measurements).filter(table_measurements.date.between(start_date, latest_date)).all()
    ###
    session.close() #Close the session
    #Take the stuff and make it a dictionary, then convert it into a JSON format.
    resultant_data_dict = {}
    #Thank you again, blackbox
    for measurement in range_of_dates:
        resultant_data_dict[measurement.date] = measurement.prcp

    return jsonify(resultant_data_dict)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)

    latest_date_query = session.query(func.max(table_measurements.date)).scalar()

    latest_date = datetime.strptime(latest_date_query, '%Y-%m-%d')
    start_date = latest_date - timedelta(days=365)

    range_of_dates = session.query(table_measurements).filter(table_measurements.date.between(start_date, latest_date)).all()

    column_names = [c.key for c in table_measurements.__table__.columns]

    df = pd.DataFrame([{k: getattr(row, k) for k in column_names} for row in range_of_dates])

    station_counts = df['station'].value_counts()
    station_counts = station_counts.to_dict() #Blackbox comin' in clutch with this line

    session.close()

    return jsonify(station_counts)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)

    latest_date_query = session.query(func.max(table_measurements.date)).scalar()

    latest_date = datetime.strptime(latest_date_query, '%Y-%m-%d')
    start_date = latest_date - timedelta(days=365)

    range_of_dates = session.query(table_measurements).filter(table_measurements.date.between(start_date, latest_date)).all()

    column_names = [c.key for c in table_measurements.__table__.columns]

    df = pd.DataFrame([{k: getattr(row, k) for k in column_names} for row in range_of_dates])

    active_station = df[df['station'] == 'USC00519397']

    return_list = [active_station['tobs'].max(), active_station['tobs'].min(), active_station['tobs'].mean()]

    return jsonify(return_list)

if __name__ == "__main__":
    app.run(debug=True)