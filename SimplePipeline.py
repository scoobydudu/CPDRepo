# Import pandas 
import pandas as pd 

# Data used in this pipeline
RETAIL_FILE = "https://raw.githubusercontent.com/elenalowery/data-samples/main/Retail_Products_and_Customers.csv"

def read_raw_data():
    retailData = pd.read_csv(RETAIL_FILE)
    return retailData

def filter_data(rawData):
    # Drop a few columns
    filteredRetailData = rawData.drop(['Buy', 'PROFESSION', 'EDUCATION'], axis=1)
    return filteredRetailData
 
def write_data_by_product_line(filteredData):
 
    # Select any product line - we will write it to a separate file
    campingEquipment = filteredData.loc[filteredData['Product line'] == 'Camping Equipment']
    
    # Write the csv file
    campingEquipment.to_csv("Camping_Equipment.csv", index=False)
 
    # Select any product line
    golfEquipment = filteredData.loc[filteredData['Product line'] == 'Golf Equipment']
 
    # Write the csv file
    golfEquipment.to_csv("Golf_Equipment.csv", index=False)

    
def prepare_retail_data():
        # Call the step job - read data
        rawData = read_raw_data()

        # Filter data
        filteredData = filter_data(rawData)

        # Write data by product line
        write_data_by_product_line(filteredData)

        print("Finished running the pipeline")


# Invoke the main function
prepare_retail_data()
