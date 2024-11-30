from flask import Flask, jsonify
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date
import pandas as pd
import datetime as dt

app = Flask(__name__)

Base = declarative_base()

class Measurement(Base):
    __tablename__ = 'measurement'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    prcp = Column(Float)  
    tobs = Column(Float)  

class Station(Base):
    __tablename__ = 'station'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String, nullable=False)
    name = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    elevation = Column(Float, nullable=False)

engine = create_engine("sqlite:///C:/Users/David/Bootcamp/homework/sqlalchemy-challenge/sqlalchemy-challenge/SurfsUp/Resources/hawaii.sqlite")

Base.metadata.create_all(engine)

def load_data():
    measurements_df = pd.read_csv('C:/Users/David/Bootcamp/homework/sqlalchemy-challenge/sqlalchemy-challenge/SurfsUp/Resources/hawaii_measurements.csv')
    
    stations_df = pd.read_csv('C:/Users/David/Bootcamp/homework/sqlalchemy-challenge/sqlalchemy-challenge/SurfsUp/Resources/hawaii_stations.csv')

    session = Session(engine)

    for index, row in measurements_df.iterrows():
        measurement = Measurement(
            station=row['station'],
            date=row['date'],
            prcp=row['prcp'],
            tobs=row['tobs']
        )
        session.add(measurement)

    for index, row in stations_df.iterrows():
        station = Station(
            station=row['station'],
            name=row['name'],
            latitude=row['latitude'],
            longitude=row['longitude'],
            elevation=row['elevation']
        )
        session.add(station)

    session.commit()
    session.close()

@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate App API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    session = Session(engine)
    
    latest_date = session.query(func.max(Measurement.date)).scalar()  
    one_year_ago = latest_date - dt.timedelta(days=365)
    
    precipitation_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= one_year_ago).all()
    
    session.close()

    precipitation_dict = {str(date): prcp for date, prcp in precipitation_data} 
    
    return jsonify(precipitation_dict)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    
    stations_data = session.query(Station.station).all()

    session.close()

    stations_list = list(map(lambda x: x[0], stations_data))
    
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    
    most_active_station = session.query(Measurement.station, func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).\
        first()[0]
    
    latest_date = session.query(func.max(Measurement.date)).scalar()  
    one_year_ago = latest_date - dt.timedelta(days=365)
    
    temp_obs = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= one_year_ago).all()

    session.close()

    temp_obs_list = list(map(lambda x: {'date': str(x[0]), 'temperature': x[1]}, temp_obs)) 
    
    return jsonify(temp_obs_list)

@app.route("/api/v1.0/")
def api_home():
    return (
        f"Welcome to the Climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

@app.route("/api/v1.0/<start>/<end>")
def stats(start, end=None):
    session = Session(engine)
    
    if not end:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).all()
    else:
        results = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).\
            filter(Measurement.date <= end).all()

    session.close()

    temp_stats = {'TMIN': results[0][0], 'TAVG': results[0][1], 'TMAX': results[0][2]}
    
    return jsonify(temp_stats)

if __name__ == "__main__":
    app.run(debug=False)