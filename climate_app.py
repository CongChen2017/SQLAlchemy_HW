from sqlalchemy import create_engine, Table, MetaData, insert, select, func, desc
from flask import Flask, jsonify
import pandas as pd


#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Welcome to the Hawaii climate API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end<br/>"

    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Database connection & reflection
    engine = create_engine("sqlite:///hawaii.sqlite")
    metadata = MetaData()
    connection = engine.connect()
    measurement = Table('measurement', metadata, autoload=True, autoload_with=engine)
    # build statement for query
    prcp_sum = func.sum(measurement.columns.prcp).label('precipitation')
    stmt = select([measurement.columns.date, prcp_sum])
    stmt = stmt.group_by(measurement.columns.date)
    stmt = stmt.where(measurement.columns.date > '2016-08-31')
    # run query save results to dictionary
    results = connection.execute(stmt).fetchall()
    results_dic = {result[0]:result[1] for result in results}

    return jsonify(results_dic)

@app.route("/api/v1.0/stations")
def stations():
    # Database connection & reflection
    engine = create_engine("sqlite:///hawaii.sqlite")
    metadata = MetaData()
    connection = engine.connect()
    station = Table('station', metadata, autoload=True, autoload_with=engine)
    # build statement for query
    stmt = select([station.columns.station.distinct()])
    # Execute the query convert results to a list
    distinct_station = connection.execute(stmt).fetchall()
    station_list = [station[0] for station in distinct_station]

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    # Database connection & reflection
    engine = create_engine("sqlite:///hawaii.sqlite")
    metadata = MetaData()
    connection = engine.connect()
    measurement = Table('measurement', metadata, autoload=True, autoload_with=engine)
    # build statement for query
    stmt = select([measurement.columns.tobs])
    stmt = stmt.where(measurement.columns.station == "USC00519281")
    stmt = stmt.where(measurement.columns.date > '2016-08-31')
    # Execute the query convert results to a list
    tobs = connection.execute(stmt).fetchall()
    tobs_list = [tob[0] for tob in tobs]

    return jsonify(tobs_list)

@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def temp_calc(start, end='2017-08-23'):
    # Database connection & reflection
    engine = create_engine("sqlite:///hawaii.sqlite")
    metadata = MetaData()
    connection = engine.connect()
    measurement = Table('measurement', metadata, autoload=True, autoload_with=engine)
    # build statement for query
    tobs_avg = func.avg(measurement.columns.tobs).label('tobs_avg')
    stmt = select([tobs_avg])
    stmt = stmt.where(measurement.columns.date > start)
    stmt = stmt.where(measurement.columns.date < end)
    stmt = stmt.group_by(measurement.columns.date)
    # Execute the query
    results = connection.execute(stmt).fetchall()
    # generate data frame and calculate
    df = pd.DataFrame(results)
    df.columns = results[0].keys()
    # calcuations
    mean_tobs = df['tobs_avg'].mean()
    max_tobs = df['tobs_avg'].max()
    min_tobs = df['tobs_avg'].min()

    return jsonify([mean_tobs, max_tobs, min_tobs])


if __name__ == "__main__":
    app.run(debug=True)