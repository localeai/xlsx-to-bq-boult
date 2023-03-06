import io
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import pandas_gbq

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from dotenv import load_dotenv

# load_dotenv()

FOLDER_ID_SHIPROCKET = os.environ.get("FOLDER_ID_SHIPROCKET")
FOLDER_ID_SHOPIFY = os.environ.get("FOLDER_ID_SHOPIFY")


def download_from_folder(name, folder_name, columns_sent, table_id, folder_id):
    creds_name = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    # Replace with the path to your service account key
    creds = service_account.Credentials.from_service_account_file(creds_name)
    drive_service = build('drive', 'v3', credentials=creds)

    # Search for files in the folder
    # q = "name='" + folder_name + "' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    q = "parents='" + folder_id + "' and trashed=false"
    results = drive_service.files().list(fields="nextPageToken, files(id, name, createdTime)", q=q).execute()
    files = results.get("files", [])

    # Sort the files by created time and pick the latest file
    files.sort(key=lambda x: x['createdTime'], reverse=True)
    print(files)
    latest_file = files[0]
    # Get the shareable link for the latest file
    file_id = latest_file['id']

    credentials = service_account.Credentials.from_service_account_file(creds_name)
    scopes = ['https://www.googleapis.com/auth/drive']
    scoped_credentials = credentials.with_scopes(scopes)

    # Build the service object
    service = build('drive', 'v3', credentials=credentials)

    try:
        # Perform the request
        request = service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logging.debug("Download %d%%." % int(status.progress() * 100))

    except HttpError as error:
        print(error)
        logging.debug(f'An error occurred: {error}')
        sys.exit()

    # Save the file to disk
    fh.seek(0)
    with open(f'./{name}.xlsx', 'wb') as f:
        f.write(fh.read())
    df = pd.read_excel(f'./{name}.xlsx', dtype=str)
    df = df.astype(str)
    project_id = os.environ.get('project_id')

    df.rename(columns=columns_sent, inplace=True)

    df['date'] = datetime.today().astimezone(tz=pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y')
    print(df)
    print(table_id, name)
    # Replace with the link to the Google Sheets document
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=creds)


# Replace with your own dataset and table name
dataset_id = os.environ.get('dataset_id')  # Specify the dataset name where you want to create the table
table_id_shiprocket = dataset_id + "." + os.environ.get(
    'table_id_shiprocket')  # Specify the table name that you want to create
table_id_shopify = dataset_id + "." + os.environ.get('table_id_shopify')

shiprocket_columns = {
    "Order ID": "Order_ID",
    "Forward ID": "Forward_ID",
    "Shiprocket Created At": "Shiprocket_Created_At",
    "Time": "Time",
    "Channel": "Channel",
    "Status": "Status",
    "Channel SKU": "Channel_SKU",
    "Master SKU": "Master_SKU",
    "Product Name": "Product_Name",
    "Product Quantity": "Product_Quantity",
    "Channel Created At": "Channel_Created_At",
    "Customer Name": "Customer_Name",
    "Customer Email": "Customer_Email",
    "Customer Mobile": "Customer_Mobile",
    "Address Line 1": "Address_Line_1",
    "Address Line 2": "Address_Line_2",
    "Address City": "Address_City",
    "Address State": "Address_State",
    "Address Pincode": "Address_Pincode",
    "Payment Method": "Payment_Method",
    "Product Price": "Product_Price",
    "Order Total": "Order_Total",
    "Tax": "Tax",
    "Tax %": "Tax_percentage",
    "Discount Value": "Discount_Value",
    "Product HSN": "Product_HSN",
    "Weight (KG)": "Weight_KG",
    "dimensions (CM)": "dimensions_CM",
    "Charged Weight": "Charged_Weight",
    "Courier Company": "Courier_Company",
    "AWB Code": "AWB_Code",
    "SRX Premium LM AWB": "SRX_Premium_LM_AWB",
    "Shipping Bill URL": "Shipping_Bill_URL",
    "AWB Assigned Date": "AWB_Assigned_Date",
    "Pickup Location ID": "Pickup_Location_ID",
    "Pickup Address Name": "Pickup_Address_Name",
    "Pickup scheduled Date": "Pickup_scheduled_Date",
    "Order Picked Up Date": "Order_Picked_Up_Date",
    "Pickup First Attempt Date": "Pickup_First_Attempt_Date",
    "Pickedup Timestamp": "Pickedup_Timestamp",
    "Order Shipped Date": "Order_Shipped_Date",
    "EDD": "EDD",
    "Delayed Reason": "Delayed_Reason",
    "Order Delivered Date": "Order_Delivered_Date",
    "RTO Address": "RTO_Address",
    "RTO Initiated Date": "RTO_Initiated_Date",
    "RTO Delivered Date": "RTO_Delivered_Date",
    "COD Remittance Date": "COD_Remittance_Date",
    "COD Payble Amount": "COD_Payble_Amount",
    "Remitted Amount": "Remitted_Amount",
    "CRF ID": "CRF_ID",
    "UTR No": "UTR_No",
    "Zone": "Zone",
    "COD Charges": "COD_Charges",
    "Freight Total Amount": "Freight_Total_Amount",
    "Customer_invoice_id": "Customer_invoice_id",
    "Shipping Charges": "Shipping_Charges",
    "Pickup Exception Reason": "Pickup_Exception_Reason",
    "First Out For Delivery Date": "First_Out_For_Delivery_Date",
    "First_Pickup_Scheduled_Date": "First_Pickup_Scheduled_Date",
    "Buyer's Lat/long": "Buyers_Lat_long",
    "Order Type": "Order_Type",
    "Order Tags": "Order_Tags",
    "Invoice Date": "Invoice_Date",
    "Eway Bill Nos": "Eway_Bill_Nos",
    "NDR 1 Attempt Date": "NDR_1_Attempt_Date",
    "NDR 2 Attempt Date": "NDR_2_Attempt_Date",
    "NDR 3 Attempt Date": "NDR_3_Attempt_Date",
    "Bridge Call Recording": "Bridge_Call_Recording",
    "Hub Address": "Hub_Address",
    "Rto Risk": "Rto_Risk",
    "RAD Datetimestamp": "RAD_Datetimestamp",
    "Pickup Pincode": "Pickup_Pincode",
    "RTO Reason": "RTO_Reason"
}

shopify_columns = {
    "Name": "Name",
    "Email": "Email",
    "Financial Status": "Financial_Status",
    "Paid at": "Paid_at",
    "Fulfillment Status": "Fulfillment_Status",
    "Fulfilled at": "Fulfilled_at",
    "Accepts Marketing": "Accepts_Marketing",
    "Currency": "Currency",
    "Subtotal": "Subtotal",
    "Shipping": "Shipping",
    "Taxes": "Taxes",
    "Total": "Total",
    "Discount Code": "Discount_Code",
    "Discount Amount": "Discount_Amount",
    "Shipping Method": "Shipping_Method",
    "Created at": "Created_at",
    "Time": "Time",
    "Lineitem quantity": "Lineitem_quantity",
    "Lineitem name": "Lineitem_name",
    "Lineitem price": "Lineitem_price",
    "Lineitem compare at price": "Lineitem_compare_at_price",
    "Lineitem sku": "Lineitem_sku",
    "Lineitem requires shipping": "Lineitem_requires_shipping",
    "Lineitem taxable": "Lineitem_taxable",
    "Lineitem fulfillment status": "Lineitem_fulfillment_status",
    "Billing Name": "Billing_Name",
    "Billing Street": "Billing_Street",
    "Billing Address1": "Billing_Address1",
    "Billing Address2": "Billing_Address2",
    "Billing Company": "Billing_Company",
    "Billing City": "Billing_City",
    "Billing Zip": "Billing_Zip",
    "Billing Province": "Billing_Province",
    "Billing Country": "Billing_Country",
    "Billing Phone": "Billing_Phone",
    "Shipping Name": "Shipping_Name",
    "Shipping Street": "Shipping_Street",
    "Shipping Address1": "Shipping_Address1",
    "Shipping Address2": "Shipping_Address2",
    "Shipping Company": "Shipping_Company",
    "Shipping City": "Shipping_City",
    "Shipping Zip": "Shipping_Zip",
    "Shipping Province": "Shipping_Province",
    "Shipping Country": "Shipping_Country",
    "Shipping Phone": "Shipping_Phone",
    "Notes": "Notes",
    "Note Attributes": "Note_Attributes",
    "Cancelled at": "Cancelled_at",
    "Payment Method": "Payment_Method",
    "Payment Reference": "Payment_Reference",
    "Refunded Amount": "Refunded_Amount",
    "Vendor": "Vendor",
    "Id": "Id",
    "Tags": "Tags",
    "Risk Level": "Risk_Level",
    "Source": "Source",
    "Lineitem discount": "Lineitem_discount",
    "Tax 1 Name": "Tax_1_Name",
    "Tax 1 Value": "Tax_1_Value",
    "Tax 2 Name": "Tax_2_Name",
    "Tax 2 Value": "Tax_2_Value",
    "Tax 3 Name": "Tax_3_Name",
    "Tax 3 Value": "Tax_3_Value",
    "Tax 4 Name": "Tax_4_Name",
    "Tax 4 Value": "Tax_4_Value",
    "Tax 5 Name": "Tax_5_Name",
    "Tax 5 Value": "Tax_5_Value",
    "Phone": "Phone",
    "Receipt Number": "Receipt_Number",
    "Duties": "Duties",
    "Billing Province Name": "Billing_Province_Name",
    "Shipping Province Name": "Shipping_Province_Name",
    "Payment ID": "Payment_ID",
    "Payment Terms Name": "Payment_Terms_Name",
    "Next Payment Due At": "Next_Payment_Due_At",
    "Payment References": "Payment_References",

}

download_from_folder(name="shopify", folder_name="Boult_shopify", columns_sent=shopify_columns,
                     table_id=table_id_shopify, folder_id=FOLDER_ID_SHOPIFY)
download_from_folder(name="shiprocket", folder_name="Boult_shiprocket", columns_sent=shiprocket_columns,
                     table_id=table_id_shiprocket,folder_id=FOLDER_ID_SHIPROCKET)
os.remove("./shiprocket.xlsx")
os.remove("./shopify.xlsx")
# df = pd.read_excel('./p.xlsx', sheet_name='Shopify', dtype=str)
# df = df.astype(str)
# Convert the dataframe to a CSV file
# df.to_csv('./final_shiprocket.csv', index=False)
# df = pd.read_csv('./final.csvv', dtype=object)

# df = df.iloc[1:]
# Replace with your own dataset and table name
# dataset_id = os.environ.get('dataset_id')  # Specify the dataset name where you want to create the table
# table_id = dataset_id + "." + os.environ.get('table_id_shopify')  # Specify the table name that you want to create
# project_id = os.environ.get('project_id')
#
# df.rename(columns={
#     "Name": "Name",
#     "Email": "Email",
#     "Financial Status": "Financial_Status",
#     "Paid at": "Paid_at",
#     "Fulfillment Status": "Fulfillment_Status",
#     "Fulfilled at": "Fulfilled_at",
#     "Accepts Marketing": "Accepts_Marketing",
#     "Currency": "Currency",
#     "Subtotal": "Subtotal",
#     "Shipping": "Shipping",
#     "Taxes": "Taxes",
#     "Total": "Total",
#     "Discount Code": "Discount_Code",
#     "Discount Amount": "Discount_Amount",
#     "Shipping Method": "Shipping_Method",
#     "Created at": "Created_at",
#     "Time": "Time",
#     "Lineitem quantity": "Lineitem_quantity",
#     "Lineitem name": "Lineitem_name",
#     "Lineitem price": "Lineitem_price",
#     "Lineitem compare at price": "Lineitem_compare_at_price",
#     "Lineitem sku": "Lineitem_sku",
#     "Lineitem requires shipping": "Lineitem_requires_shipping",
#     "Lineitem taxable": "Lineitem_taxable",
#     "Lineitem fulfillment status": "Lineitem_fulfillment_status",
#     "Billing Name": "Billing_Name",
#     "Billing Street": "Billing_Street",
#     "Billing Address1": "Billing_Address1",
#     "Billing Address2": "Billing_Address2",
#     "Billing Company": "Billing_Company",
#     "Billing City": "Billing_City",
#     "Billing Zip": "Billing_Zip",
#     "Billing Province": "Billing_Province",
#     "Billing Country": "Billing_Country",
#     "Billing Phone": "Billing_Phone",
#     "Shipping Name": "Shipping_Name",
#     "Shipping Street": "Shipping_Street",
#     "Shipping Address1": "Shipping_Address1",
#     "Shipping Address2": "Shipping_Address2",
#     "Shipping Company": "Shipping_Company",
#     "Shipping City": "Shipping_City",
#     "Shipping Zip": "Shipping_Zip",
#     "Shipping Province": "Shipping_Province",
#     "Shipping Country": "Shipping_Country",
#     "Shipping Phone": "Shipping_Phone",
#     "Notes": "Notes",
#     "Note Attributes": "Note_Attributes",
#     "Cancelled at": "Cancelled_at",
#     "Payment Method": "Payment_Method",
#     "Payment Reference": "Payment_Reference",
#     "Refunded Amount": "Refunded_Amount",
#     "Vendor": "Vendor",
#     "Id": "Id",
#     "Tags": "Tags",
#     "Risk Level": "Risk_Level",
#     "Source": "Source",
#     "Lineitem discount": "Lineitem_discount",
#     "Tax 1 Name": "Tax_1_Name",
#     "Tax 1 Value": "Tax_1_Value",
#     "Tax 2 Name": "Tax_2_Name",
#     "Tax 2 Value": "Tax_2_Value",
#     "Tax 3 Name": "Tax_3_Name",
#     "Tax 3 Value": "Tax_3_Value",
#     "Tax 4 Name": "Tax_4_Name",
#     "Tax 4 Value": "Tax_4_Value",
#     "Tax 5 Name": "Tax_5_Name",
#     "Tax 5 Value": "Tax_5_Value",
#     "Phone": "Phone",
#     "Receipt Number": "Receipt_Number",
#     "Duties": "Duties",
#     "Billing Province Name": "Billing_Province_Name",
#     "Shipping Province Name": "Shipping_Province_Name",
#     "Payment ID": "Payment_ID",
#     "Payment Terms Name": "Payment_Terms_Name",
#     "Next Payment Due At": "Next_Payment_Due_At",
#     "Payment References": "Payment_References",
#
# }, inplace=True)
#
# df['date'] = datetime.today().astimezone(tz=pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y')
# print(df)
#
# # Replace with the link to the Google Sheets document
# pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=creds)
#
# df = pd.read_excel('./p.xlsx', sheet_name='Xpressbees', dtype=str)
# df = df.astype(str)
# # Convert the dataframe to a CSV file
# # df.to_csv('./final_shiprocket.csv', index=False)
# # df = pd.read_csv('./final.csvv', dtype=object)
#
# # df = df.iloc[1:]
# # Replace with your own dataset and table name
# dataset_id = os.environ.get('dataset_id')  # Specify the dataset name where you want to create the table
# table_id = dataset_id + "." + os.environ.get('table_id_Xpressbees')  # Specify the table name that you want to create
# project_id = os.environ.get('project_id')
#
# df.rename(columns={
#     "Order ID": "Order_ID",
#     "Forward ID": "Forward_ID",
#     "Shiprocket Created At": "Shiprocket_Created_At",
#     "Time": "Time",
#     "Channel": "Channel",
#     "Status": "Status",
#     "Channel SKU": "Channel_SKU",
#     "Master SKU": "Master_SKU",
#     "Product Name": "Product_Name",
#     "Product Quantity": "Product_Quantity",
#     "Channel Created At": "Channel_Created_At",
#     "Customer Name": "Customer_Name",
#     "Customer Email": "Customer_Email",
#     "Customer Mobile": "Customer_Mobile",
#     "Address Line 1": "Address_Line_1",
#     "Address Line 2": "Address_Line_2",
#     "Address City": "Address_City",
#     "Address State": "Address_State",
#     "Address Pincode": "Address_Pincode",
#     "Payment Method": "Payment_Method",
#     "Product Price": "Product_Price",
#     "Order Total": "Order_Total",
#     "Tax": "Tax",
#     "Tax %": "Tax_percentage",
#     "Discount Value": "Discount_Value",
#     "Product HSN": "Product_HSN",
#     "Weight (KG)": "Weight_KG",
#     "dimensions (CM)": "dimensions_CM",
#     "Charged Weight": "Charged_Weight",
#     "Courier Company": "Courier_Company",
#     "AWB Code": "AWB_Code",
#     "SRX Premium LM AWB": "SRX_Premium_LM_AWB",
#     "Shipping Bill URL": "Shipping_Bill_URL",
#     "AWB Assigned Date": "AWB_Assigned_Date",
#     "Pickup Location ID": "Pickup_Location_ID",
#     "Pickup Address Name": "Pickup_Address_Name",
#     "Pickup scheduled Date": "Pickup_scheduled_Date",
#     "Order Picked Up Date": "Order_Picked_Up_Date",
#     "Pickup First Attempt Date": "Pickup_First_Attempt_Date",
#     "Pickedup Timestamp": "Pickedup_Timestamp",
#     "Order Shipped Date": "Order_Shipped_Date",
#     "EDD": "EDD",
#     "Delayed Reason": "Delayed_Reason",
#     "Order Delivered Date": "Order_Delivered_Date",
#     "RTO Address": "RTO_Address",
#     "RTO Initiated Date": "RTO_Initiated_Date",
#     "RTO Delivered Date": "RTO_Delivered_Date",
#     "COD Remittance Date": "COD_Remittance_Date",
#     "COD Payble Amount": "COD_Payble_Amount",
#     "Remitted Amount": "Remitted_Amount",
#     "CRF ID": "CRF_ID",
#     "UTR No": "UTR_No",
#     "Zone": "Zone",
#     "COD Charges": "COD_Charges",
#     "Freight Total Amount": "Freight_Total_Amount",
#     "Customer_invoice_id": "Customer_invoice_id",
#     "Shipping Charges": "Shipping_Charges",
#     "Pickup Exception Reason": "Pickup_Exception_Reason",
#     "First Out For Delivery Date": "First_Out_For_Delivery_Date",
#     "First_Pickup_Scheduled_Date": "First_Pickup_Scheduled_Date",
#     "Buyer's Lat/long": "Buyers_Lat_long",
#     "Order Type": "Order_Type",
#     "Order Tags": "Order_Tags",
#     "Invoice Date": "Invoice_Date",
#     "Eway Bill Nos": "Eway_Bill_Nos",
#     "NDR 1 Attempt Date": "NDR_1_Attempt_Date",
#     "NDR 2 Attempt Date": "NDR_2_Attempt_Date",
#     "NDR 3 Attempt Date": "NDR_3_Attempt_Date",
#     "Bridge Call Recording": "Bridge_Call_Recording",
#     "Hub Address": "Hub_Address",
#     "Rto Risk": "Rto_Risk",
#     "RAD Datetimestamp": "RAD_Datetimestamp",
#     "Pickup Pincode": "Pickup_Pincode",
#     "RTO Reason": "RTO_Reason"
# }, inplace=True)
#
# df['date'] = datetime.today().astimezone(tz=pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y')
# print(df)
#
# # Replace with the link to the Google Sheets document
# # pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=creds)
# os.remove("./x.csv")
