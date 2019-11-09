import numpy as np
import sqlalchemy
import ast
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, scoped_session, sessionmaker
from sqlalchemy import create_engine, func
import datetime as dt
from flask import Flask, jsonify
import pandas as pd

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
Base.classes.keys()

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#session=Session(engine)

session = scoped_session(sessionmaker(bind=engine))
#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return(
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():

    #Convert the query results to a Dictionary using `date` 
    #as the key and `prcp` as the value.
    #Return the JSON representation of your dictionary.'''

    fromdb = session.query(Measurement.date, Measurement.prcp)
    prcp_list = []
    for item in fromdb:
        prcp_dict = {}
        date = item[0]
        prcp = item[1]
        prcp_dict[date]=prcp
        prcp_list.append(prcp_dict)

    return jsonify(prcp_list)

@app.route("/api/v1.0/stations")
def stations():
    '''Return a JSON list of stations from the dataset.'''

    stationlist = engine.execute("SELECT* FROM Station").fetchall()
    stations_list = []

    import ast

    for stations in stationlist:
        station_id=stations[1]
        name = stations[2]
        lat = stations[3]
        lon = stations[4]
        elevation = stations[5]
        dictz="{'station_id':'" + station_id + "','station_name':'" + name +"','lat':" + str(lat) + ",'lon':" + str(lon) + ",'elevation':" + str(elevation) + "}"
        res = ast.literal_eval(dictz)
        stations_list.append(res)
    
    return jsonify(stations_list)

@app.route("/api/v1.0/tobs")
    #query for the dates and temperature observations from a year from the last data point.
     #Return a JSON list of Temperature Observations (tobs) for the previous year.

def tobs():
    
    stationdata_df = pd.DataFrame(engine.execute("SELECT s.station, s.name, m.date, m.prcp, m.tobs FROM Station S LEFT JOIN Measurement M WHERE s.station = m.station ORDER BY s.name").fetchall())
    stationdata_df.columns = ['station','name','date','prcp','tobs']

    station_activity = stationdata_df.groupby('station', as_index=False)['name'].count()
    station_activity = station_activity.sort_values('name',ascending=False)

    highest_activity = station_activity.iloc[0,0]

    last_date = session.query(func.max(Measurement.date)).first()

    date = ''
    date = last_date
    for x in date:
        assets = x.split("-")
        year = int(assets[0])
        month = int(assets[1])
        day = int(assets[2])

        ''' Calculate the date 1 year ago from the last data point in the database '''
    query_date = dt.date(year,month,day) - dt.timedelta(days=365)

    tobs_query =session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= query_date).filter(Measurement.station == highest_activity).all()

    tobs_list = []
    for item in tobs_query:
        tobs_dict = {}
        date = item[0]
        tobs = item[1]
        tobs_dict[date]=tobs
        tobs_list.append(tobs_dict)

    return jsonify(tobs_list)

# * `/api/v1.0/<start>` and `/api/v1.0/<start>/<end>`

#   * Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.

#   * When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date.

#   * When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive.

if __name__ == "__main__":
    app.run(debug=True)