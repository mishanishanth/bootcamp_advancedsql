from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from datetime import datetime

import pandas as pd

# Define the SQLHelper Class
# PURPOSE: Deal with all of the database logic

class SQLHelper():

    # Initialize PARAMETERS/VARIABLES

    #################################################
    # Database Setup
    #################################################
    def __init__(self):
        #self.engine = create_engine("sqlite:///hawaii.sqlite")
        #self.engine = create_engine("sqlite:/Starter_Code/Resources/hawaii.sqlite")
        self.engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        self.Measurement = self.createMeasurement()
        self.Station = self.createstation()
        
    def createMeasurement(self):
        # Reflect an existing database into a new model
        Base = automap_base()

        # reflect the tables
        Base.prepare(autoload_with=self.engine)

        # Save reference to the table
        Measurement = Base.classes.measurement
        return(Measurement)
    
    def createstation(self):
        # Reflect an existing database into a new model
        Base = automap_base()

        # reflect the tables
        Base.prepare(autoload_with=self.engine)

        # Save reference to the table
        Station = Base.classes.station
        return(Station)
         
    def queryMeasurement(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        
        rows = session.query(self.Measurement.date, self.Measurement.prcp).\
        filter(self.Measurement.date>='2016-08-23').all()

        # Create the dataframe
        df = pd.DataFrame(rows)

        # Close the Session
        session.close()
        return(df)

    def queryStation(self):
        # Create our session (link) from Python to the DB
        session = Session(self.engine)

        station_name = session.query(self.Station.station).all()
        df_stationname = pd.DataFrame(station_name)
        session.close()
        return(df_stationname)
    
    def queryTemperature(self):
        session = Session(self.engine)
        station_maxrowfirst = session.query(self.Measurement.station, func.count(self.Measurement.id).label('count')) \
        .group_by(self.Measurement.station) \
        .order_by(func.count(self.Measurement.id).desc()).first()
     #   Get the station ID 
        most_active_station = station_maxrowfirst[0]
        temp_one_year = session.query(self.Measurement.station,self.Measurement.date,self.Measurement.tobs).\
                  filter(self.Measurement.station == most_active_station).filter(self.Measurement.date > '2016-08-23').\
                    order_by(self.Measurement.date).all()
        temp_df = pd.DataFrame(temp_one_year)
        session.close()
        return temp_df
    
    def tempstatisticsstart(self,start):
        session = Session(self.engine)
        start = datetime.strptime(start, '%Y-%m-%d').date()
        tempstat =session.query(func.min(self.Measurement.tobs),func.max(self.Measurement.tobs),\
        func.avg(self.Measurement.tobs)).filter(self.Measurement.date >= start).all()
      
        tempstat_df = pd.DataFrame(tempstat, columns=['TMIN', 'TMAX', 'TAVG'])
        session.close()
        return(tempstat_df)
    
    def tempstatstartend(self,start,end):
        session = Session(self.engine)
        start = datetime.strptime(start, '%Y-%m-%d').date()
        end = datetime.strptime(end, '%Y-%m-%d').date()
        tempstatstartends =session.query(func.min(self.Measurement.tobs),func.max(self.Measurement.tobs),\
        func.avg(self.Measurement.tobs)).filter(self.Measurement.date >= start).filter(self.Measurement.date <= end)
        tempstatstartend_df = pd.DataFrame(tempstatstartends, columns=['TMIN', 'TMAX', 'TAVG'])
        session.close()
        return(tempstatstartend_df)

    
    


    