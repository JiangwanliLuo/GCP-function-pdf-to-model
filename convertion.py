import pandas as pd
import re
def convert_cols_positive(df, cols):
    for col in cols:
        df[col] = df[col].abs()
    return df

def convert_cols_negtive(df, cols):
    for col in cols:
        df[col] = -df[col].abs()
    return df

def schema_convert(df_header,df_items):
    # Convert the columns to the desired types
    convert_dataString_to_date(df_header,"InvoiceDate")
    header_float_list = ['NetAmount','TaxAmount','InvoiceTotal','Freight']
    df_header = header_item_columns_float_convert(df_header,header_float_list)
    df_header.rename(columns={'Freight': 'FreightIncGST'}, inplace=True)
    df_header['Freight'] = (df_header['FreightIncGST'].replace('[\$,]', '', regex=True).astype(float) / 11).round(4)
    df_header['FreightGST'] = (df_header['FreightIncGST'].replace('[\$,]', '', regex=True).astype(float) / 11 * 0.1).round(4)
    header_string_columns = ['InvoiceID', 'InvoiceType', 'InvoiceNo', 'PoNo', 'CustomerAccount', 
                    'SupplierFull', 'ABN', 'CustomerName']
    df_header[header_string_columns] = df_header[header_string_columns].astype(str) 
    update_net_amount(df_header)
    # List of required columns
    if not df_items.empty: 
        item_required_columns = [
            'SupplierProdCode',
            'ProductDescription',
            'QuantityInvoiced',
            'UOM',
            'UnitPrice',
            'ExtendedLineTotal',
            'LineItemGST',
            'ExtendedGST'  # This column may not exist yet
        ] 

        # Ensure all required columns are in the DataFrame
        for column in item_required_columns:
            if column not in df_items.columns:
                # Check if the column should be float or string
                if column in ['QuantityInvoiced', 'UnitPrice', 'ExtendedLineTotal', 'LineItemGST', 'ExtendedGST']:
                    df_items[column] = 0.0  # Add float columns with 0.0 as the default value
                else:
                    df_items[column] = 'NA'  # Add string columns with 'NA' as the default value
        
        # List of columns that should be converted to float
        items_float_list = ['QuantityInvoiced', 'UnitPrice', 'ExtendedLineTotal', 'LineItemGST', 'ExtendedGST']
        
        # Convert specified columns to float
        df_items = header_item_columns_float_convert(df_items, items_float_list)
    return df_header,df_items 

def update_unit_price_and_gst(row):
    # Set QuantityInvoiced to 1 if it's None, otherwise keep its value
    quantity = row['QuantityInvoiced'] if row['QuantityInvoiced'] is not None else 1

    # Calculate the expected extended line total
    expected_total = (row['UnitPrice'] + row['LineItemGST']) * quantity

    # Check if the difference is within the tolerance range
    if abs(row['ExtendedLineTotal'] - expected_total) < 0.5:
        # Return the original values if within tolerance
        return row['UnitPrice'], row['LineItemGST'], row['ExtendedGST'], quantity
    else:
        # Calculate new values if not within tolerance
        new_unit_price = (row['ExtendedLineTotal'] - row['ExtendedGST']) / quantity
        new_gst = row['ExtendedGST'] / quantity

        # Return the recalculated values
        return new_unit_price, new_gst, row['ExtendedGST'], quantity
    

def update_net_amount(df):
    # Update NetAmount: if it's 0, set it to InvoiceTotal - TaxAmount, otherwise keep the original value
    df['NetAmount'] = df['NetAmount'].fillna(0)
    df['NetAmount'] = df.apply(
        lambda row: row['InvoiceTotal'] - row['TaxAmount'] if row['NetAmount'] == 0 else row['NetAmount'], axis=1
    )
    return df


def check_invoice_type(invoice_type, invoice_total):
    invoice_type_cleaned = str(invoice_type).strip().lower()
    if any(keyword in invoice_type_cleaned for keyword in ["creditnote", "adjustment"]) or invoice_total < 0:
        return "CRNote"
    else:
        return "INVOICE"
    

def detect_CN(df_header, df_items):
    # Update 'InvoiceType' based on a check function for each row
    df_header['InvoiceType'] = df_header.apply(lambda row: check_invoice_type(row['InvoiceType'], row['InvoiceTotal']), axis=1)

    # Check if 'InvoiceType' is 'CRNote' and process accordingly
    if df_header['InvoiceType'].iloc[0] == 'CRNote':
        columns_to_negate = ['InvoiceTotal', 'TaxAmount']
        df_header = convert_cols_negtive(df_header, columns_to_negate)

        if df_items.empty:
            return df_header, pd.DataFrame()
        else:
            items_to_negate = ['QuantityInvoiced']
            df_items = convert_cols_positive(df_items, items_to_negate)
            return df_header, df_items

    # Return unmodified DataFrames if 'InvoiceType' is not 'CRNote'
    return df_header, df_items


def convert_dataString_to_date(df,date_column):
    date_formats = [
        "%d/%m/%Y",  # 30/04/2023
        "%d/%m/%y",  # 30/04/23
        "%d-%b-%Y",  # 15-May-2023
        "%d-%b-%y",  # 15-May-23
        "%Y-%m-%d",  # 2023-01-15
        "%y-%m-%d",  # 23-01-15
        "%d %b %Y"   # 17 Sep 2024
      ]
    # Iterate over each format and try converting
    for fmt in date_formats:
        try:
            df[date_column] = pd.to_datetime(df[date_column], format=fmt, dayfirst=True)
            df[date_column] = pd.to_datetime(df[date_column], unit='ms').dt.strftime('%Y%m%d')
            break  # Break the loop if successful
        except ValueError:
            pass 
        
def clean_and_convert_to_float(value): 
    if pd.isnull(value):
        return 0  # Return NaN if the value is null
    
    # Check if the value is a string
    if isinstance(value, str):
        # Remove any commas, dollar signs, or spaces
        cleaned_value = re.sub(r'[^\d.-]', '', value)
        
        # Attempt to convert to float
        try:
            return float(cleaned_value)
        except ValueError:
            return 0  # Return NaN if conversion fails
    else:
        return float(value)

def header_item_columns_float_convert(df,float_column_list):
  for col in float_column_list:
    if col not in df.columns:
        print(f"Column '{col}' is not present in the DataFrame.")
    else:
        df[col] = df[col].apply(clean_and_convert_to_float)
  return df
