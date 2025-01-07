# Import the dependencies.
import pandas as pd
from flask import Flask, jsonify
from sql_helper import SQLHelper

#################################################
# Flask Setup
#################################################
app = Flask(__name__)
sqlHelper = SQLHelper()


#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
       
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Execute queries
    df = sqlHelper.queryMeasurement()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/stations")
def station():
    # Execute queries
    df = sqlHelper.queryStation()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/tobs")
def temperature():
    # Execute queries
    df = sqlHelper.queryTemperature()

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>", methods=["GET"])
def starttemp(start):
     # Execute queries
    df = sqlHelper.tempstatisticsstart(start)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

@app.route("/api/v1.0/<start>/<end>", methods=["GET"])
def startendtemp(start,end):
     # Execute queries
    df = sqlHelper.tempstatstartend(start,end)

    # Turn DataFrame into List of Dictionary
    data = df.to_dict(orient="records")
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)


