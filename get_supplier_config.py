from google.cloud import bigquery
import os
from convertion import update_unit_price_and_gst
import importlib

# Dynamically import the module
module_name = 'additonal_label'
module = importlib.import_module(module_name)

project_id = 'cc-doc-ai-data-extract'
dataset_id = 'azure_model_config'
table_id = 'ItemModel_AdditionalLabels'

# Define the SQL query

# credential_path = ""
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

def get_data_from_bigquery(sql_query):
    # Initialize BigQuery client

    client = bigquery.Client()

    # Execute the SQL query
    query_job = client.query(sql_query)

    # Wait for the query to complete
    df = query_job.to_dataframe()

    return df



def get_unique_send_received_codes(df):

    # Check if 'Send_Received_Code' exists in the DataFrame
    if 'Send_Received_Code' not in df.columns:
        raise ValueError("DataFrame does not contain a 'Send_Received_Code' column.")
    
    # Get unique codes
    unique_codes = df['Send_Received_Code'].unique()
    
    # Get the number of unique codes
    num_unique_codes = len(unique_codes)
    
    return num_unique_codes, unique_codes.tolist()

def process_send_received_codes(num_unique_codes, unique_codes_list, siteaccount):

    if  num_unique_codes == 1:
        return unique_codes_list[0]  # Return the only unique code
    elif num_unique_codes > 1:
        unique_codes_str = ', '.join(f"{code}" for code in unique_codes_list)
        # print(unique_codes_str)
        return update_srcode_base_on_siteaccount(siteaccount, unique_codes_str)
    else:
        raise ValueError("Error: No set up supplier config for model or Invalid number of unique codes.")

def update_srcode_base_on_siteaccount(siteaccount, unique_codes_str):
    # BigQuery SQL query
    query = f"""
        SELECT DISTINCT *
        FROM `cc-doc-ai-data-extract.cc_docai_invoice_extract.supplier_costcentre`
        WHERE CustomerAccount = '{siteaccount}' AND SRcode IN ({unique_codes_str}) LIMIT 1
    """
    # Execute query
    df_site_info = get_data_from_bigquery(query)

    # Check if data was returned
    if not df_site_info.empty:
        return df_site_info['SRcode'].iloc[0]  # Return SRcode if data exists
    else:
        # If no data returned, use the first item in unique_codes_str
        first_code = unique_codes_str.split(",")[0].strip()
        
        # Convert first_code to float64
        try:
            first_code_float = float(first_code)
        except ValueError:
            raise ValueError(f"Cannot convert {first_code} to float.")
        
        return first_code_float

def update_freight_base_supplier_config(df_header, Freight_Included_GST):
    # Check if Freight_Included_GST is True
    if Freight_Included_GST:
        # No change to df_header
        df_header_updated = df_header
    else:
        # Update Freight column in df_header
        df_header_updated = df_header.copy()  # Create a copy to avoid modifying the original
        df_header_updated['Freight'] = (df_header_updated['Freight'] * 1.1).round(4)
    return df_header_updated

def update_UnitPrice_base_supplier_config(df_item, Unit_Price_Included_GST, Model_Level):
    # Check if Unit_Price_Included_GST is True and Model_Level is 'Item'
    if Unit_Price_Included_GST == True and Model_Level == 'Item':
        # Update UnitPrice column in df_item
        df_item_updated = df_item.copy()  # Create a copy to avoid modifying the original
        df_item_updated['UnitPrice'] = (df_item_updated['UnitPrice'] / 1.1).round(4)
        return df_item_updated
    else:
        # No change to df_item
        df_item_updated = df_item
        return df_item_updated




def update_df_base_supplier_config(df_header,df_items, model_id):
    
    query = f"""
        SELECT *
        FROM `cc-doc-ai-data-extract.azure_model_config.supplier_setting`
        WHERE Model_ID != ''
        and Model_ID = '{model_id}'
    """

    query_addittional_label = f"""
        SELECT *
        FROM `cc-doc-ai-data-extract.azure_model_config.supplierconfig_additionallabels`
        WHERE ModelID != ''
        and ModelID = '{model_id}'
    """
    df_setting = get_data_from_bigquery(query)
    df_addition_label = get_data_from_bigquery(query_addittional_label)

    num_unique_codes,unique_codes_list =  get_unique_send_received_codes(df_setting)
    siteaccount = df_header['CustomerAccount'].iloc[0]
    # siteaccount = 'CCS_BAU'

    updated_SRcode = process_send_received_codes(num_unique_codes, unique_codes_list, siteaccount)
    filtered_df_setting = df_setting[df_setting['Send_Received_Code'] == updated_SRcode]
    print(filtered_df_setting)

    Freight_Included_GST = filtered_df_setting['Freight_Included_GST'].iloc[0]
    Unit_Price_Included_GST = filtered_df_setting['Unit_Price_Included_GST'].iloc[0]
    Model_Level = filtered_df_setting['Model_Level'].iloc[0]

    print(f"Freight Included (GST): {Freight_Included_GST}")
    print(f"Unit Price Included (GST): {Unit_Price_Included_GST}")
    print(f"Model Level: {Model_Level}")


    df_header['Send_Received_Code'] = updated_SRcode
    df_header = update_freight_base_supplier_config(df_header, Freight_Included_GST)
    if not df_items.empty:
        df_items = update_UnitPrice_base_supplier_config(df_items, Unit_Price_Included_GST, Model_Level)

        AdditionalLabels = df_addition_label["AdditionalLabels"]
        for label in AdditionalLabels:
            if hasattr(module, label):  # Check if the label corresponds to a valid function
                function_to_call = getattr(module, label)  # Get the function by label name
                
                # Call the function on the entire DataFrame, passing `label` if necessary
                df_items = function_to_call(df_items, label)

        df_items[['UnitPrice', 'LineItemGST', 'ExtendedGST', 'QuantityInvoiced']] = df_items.apply(update_unit_price_and_gst, axis=1, result_type='expand')
        

    return df_header, df_items
                
