import pandas as pd

# Example functions
def GSTcode(df_items, column_name):
    print("Function GSTcode called")
    # Check if the specified column exists in the DataFrame
    if column_name not in df_items.columns:
        
        return  print(f"Column '{column_name}' does not exist in the DataFrame.")
    
    # Create a new column for LineItemGST
    df_items['LineItemGST'] = 0  # Initialize LineItemGST column
    df_items['ExtendedGST'] = 0 
    
    # Apply the GST code logic
    for index, row in df_items.iterrows():
        if row[column_name] == 'FRE':
            # No change
            df_items.at[index, 'LineItemGST'] = row.get('LineItemGST', 0) 
            df_items.at[index, 'ExtendedGST'] = row.get('ExtendedGST', 0)  # Retain existing value, default to 0
             # Retain existing value, default to 0
        elif row[column_name] == 'GST':
            # Calculate GST as 10% of ExtendedLineTotal
            df_items.at[index, 'LineItemGST'] = row.get('UnitPrice', 0) * 0.1
            df_items.at[index, 'ExtendedGST'] = row.get('ExtendedLineTotal', 0) /11

            
    return df_items.drop(columns=['GSTcode'])

def B():
    print("Function B called")
    return "B result"

def C():
    print("Function C called")
    return "C result"

def D():
    print("Function D called")
    return "D result"

