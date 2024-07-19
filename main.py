import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from docx import Document
import fitz
from databricks import sql
from botocore.exceptions import ClientError

# Streamlit application title
st.title('AWS and Databricks File and Database Management')

# Main selector for choosing between AWS and Databricks
operation_type = st.sidebar.selectbox('Choose Operation Type', ['AWS', 'Databricks'])

if operation_type == 'AWS':
    # Input AWS configuration
    st.sidebar.title('AWS Configuration')
    AWS_ACCESS_KEY_ID = st.sidebar.text_input('AWS Access Key ID')
    AWS_SECRET_ACCESS_KEY = st.sidebar.text_input('AWS Secret Access Key', type='password')
    AWS_REGION = st.sidebar.text_input('AWS Region')
    BUCKET_NAME = st.sidebar.text_input('S3 Bucket Name')

    # Create S3 client if AWS credentials are provided
    if all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME]):
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

    # Get S3 file list
    def list_files_in_bucket(bucket_name, folder):
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
            files = response.get('Contents', [])
            file_list = [{'Key': file['Key'], 'Size': file['Size']} for file in files if
                         file['Key'] != folder and not file['Key'].endswith('/')]
            return file_list
        except ClientError as e:
            st.error(f"An error occurred: {e}")
            return []

    # Download file from S3
    def download_file(bucket_name, file_key):
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            return response['Body'].read()
        except ClientError as e:
            st.error(f"An error occurred while downloading the file: {e}")
            return None

    # Upload file to S3
    def upload_file(bucket_name, file, folder):
        try:
            file_key = folder + file.name
            s3_client.upload_fileobj(file, bucket_name, file_key)
            st.success(f"File {file.name} uploaded successfully.")
        except ClientError as e:
            st.error(f"An error occurred while uploading the file: {e}")

    # Display CSV content
    def display_database_content(content):
        data = pd.read_csv(BytesIO(content))
        st.dataframe(data)

    # Display Word document content
    def display_word_content(content):
        doc = Document(BytesIO(content))
        for para in doc.paragraphs:
            st.write(para.text)

    # Display PDF content
    def display_pdf_content(content):
        pdf_document = fitz.open(stream=content, filetype="pdf")
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            page_text = page.get_text("text")
            st.write(page_text)

    # Streamlit application for AWS operations
    st.sidebar.title('AWS File Operations')
    aws_operation = st.sidebar.selectbox('Choose AWS Operation', ['View Files', 'Upload File'])

    aws_folder_path = st.text_input("Enter the S3 folder path:", "")

    if aws_folder_path:
        if aws_operation == 'View Files':
            st.header('View and Download Files')
            files = list_files_in_bucket(BUCKET_NAME, aws_folder_path)
            file_options = [file['Key'] for file in files]

            if file_options:
                selected_file = st.selectbox('Select a file', file_options)

                if selected_file:
                    st.write(f'Selected file: {selected_file}')
                    preview_clicked = st.button('Preview File')
                    download_clicked = st.button('Download File')

                    if preview_clicked:
                        content = download_file(BUCKET_NAME, selected_file)
                        if content:
                            st.subheader('File Content')
                            try:
                                if selected_file.endswith('.csv'):
                                    display_database_content(content)
                                elif selected_file.endswith('.pdf'):
                                    display_pdf_content(content)
                                elif selected_file.endswith('.docx'):
                                    display_word_content(content)
                                else:
                                    st.write("Unsupported file type.")
                            except Exception as e:
                                st.write("Unable to display file content.")
                                st.write(e)

                    if download_clicked:
                        content = download_file(BUCKET_NAME, selected_file)
                        if content:
                            st.download_button(
                                label='Download file',
                                data=content,
                                file_name=selected_file.split('/')[-1]
                            )
            else:
                st.write("No files found in the specified folder.")

        elif aws_operation == 'Upload File':
            st.header('Upload a File')
            uploaded_file = st.file_uploader('Choose a file', type=['csv', 'pdf', 'docx'])

            if uploaded_file is not None:
                content = uploaded_file.read()
                upload_file(BUCKET_NAME, uploaded_file, aws_folder_path)

                # Preview the uploaded file
                st.subheader('File Preview')
                try:
                    if uploaded_file.name.endswith('.csv'):
                        display_database_content(content)
                    elif uploaded_file.name.endswith('.pdf'):
                        display_pdf_content(content)
                    elif uploaded_file.name.endswith('.docx'):
                        display_word_content(content)
                    else:
                        st.write("Unsupported file type.")
                except Exception as e:
                    st.write("Unable to display file content.")
                    st.write(e)

elif operation_type == 'Databricks':
    # Input Databricks configuration
    st.sidebar.title('Databricks Configuration')
    DATABRICKS_SERVER_HOSTNAME = st.sidebar.text_input('Databricks Server Hostname')
    DATABRICKS_HTTP_PATH = st.sidebar.text_input('Databricks HTTP Path')
    DATABRICKS_ACCESS_TOKEN = st.sidebar.text_input('Databricks Access Token', type='password')

    # Query Databricks and display results
    def query_databricks(sql_query):
        with sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_ACCESS_TOKEN
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)
                return df

    # List tables in Databricks database
    def list_databricks_tables(database):
        query = f"SHOW TABLES IN {database}"
        return query_databricks(query)

    # Upload dummy data to Databricks table
    def upload_dummy_data(database, table, data):
        for index, row in data.iterrows():
            values = ', '.join([f"'{value}'" for value in row])
            insert_query = f"""
            INSERT INTO {database}.{table} VALUES ({values})
            """
            query_databricks(insert_query)

    # Function to read CSV file with different encodings
    def read_csv_file(uploaded_file):
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        for encoding in encodings:
            try:
                return pd.read_csv(uploaded_file, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except pd.errors.EmptyDataError:
                st.error("Uploaded file is empty.")
                return None
        st.error("Unable to read the file with utf-8, latin1, or iso-8859-1 encodings.")
        return None

    # Streamlit application for Databricks operations
    st.sidebar.title('Databricks Operations')
    databricks_operation = st.sidebar.selectbox('Choose Databricks Operation', ['List Tables', 'Upload Dummy Data'])

    databricks_database_path = st.text_input("Enter the Databricks database name:", "")

    if databricks_operation == 'List Tables' and databricks_database_path:
        st.header('List and View Databricks Tables')
        tables = list_databricks_tables(databricks_database_path)
        if not tables.empty:
            table_options = tables['tableName'].tolist()

            if table_options:
                selected_table = st.selectbox('Select a table', table_options)

                if selected_table:
                    st.write(f'Selected table: {selected_table}')
                    view_table_clicked = st.button('View Table')

                    if view_table_clicked:
                        table_query = f"SELECT * FROM {databricks_database_path}.{selected_table} LIMIT 1000"
                        table_df = query_databricks(table_query)
                        st.dataframe(table_df)
        else:
            st.write("No tables found in the specified database.")

    elif databricks_operation == 'Upload Dummy Data' and databricks_database_path:
        st.header('Upload Dummy Data to Databricks Table')
        tables = list_databricks_tables(databricks_database_path)
        if not tables.empty:
            table_options = tables['tableName'].tolist()

            if table_options:
                selected_table = st.selectbox('Select a table to upload data', table_options)

                if selected_table:
                    uploaded_file = st.file_uploader('Choose a CSV file to upload as dummy data', type='csv')

                    if uploaded_file is not None:
                        dummy_data = read_csv_file(uploaded_file)
                        if dummy_data is not None:
                            st.dataframe(dummy_data)

                            upload_clicked = st.button('Upload Data')

                            if upload_clicked:
                                try:
                                    upload_dummy_data(databricks_database_path, selected_table, dummy_data)
                                    st.success(f"Data uploaded to {selected_table} successfully.")
                                except Exception as e:
                                    st.error(f"An error occurred while uploading data: {e}")
