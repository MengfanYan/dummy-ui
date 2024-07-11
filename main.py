import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from docx import Document
import fitz
# from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.title('AWS File Management')

# 输入 AWS 配置信息
st.sidebar.title('AWS Configuration')

AWS_ACCESS_KEY_ID = st.sidebar.text_input('AWS Access Key ID')
AWS_SECRET_ACCESS_KEY = st.sidebar.text_input('AWS Secret Access Key', type='password')
AWS_REGION = st.sidebar.text_input('AWS Region')
BUCKET_NAME = st.sidebar.text_input('S3 Bucket Name')

if all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME]):

    # 创建 S3 客户端
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


# 获取 S3 文件列表
def list_files_in_bucket(bucket_name, folder):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)
    files = response.get('Contents', [])
    file_list = [{'Key': file['Key'], 'Size': file['Size']} for file in files if
                 file['Key'] != folder and not file['Key'].endswith('/')]
    return file_list


# 下载文件
def download_file(bucket_name, file_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    return response['Body'].read()


# 上传文件
def upload_file(bucket_name, file, folder):
    file_key = folder + file.name
    s3_client.upload_fileobj(file, bucket_name, file_key)
    st.success(f"File {file.name} uploaded successfully.")


# 显示数据库内容
def display_database_content(content):
    data = pd.read_csv(BytesIO(content))
    st.dataframe(data)


# 读取并显示 Word 文件内容
def display_word_content(content):
    doc = Document(BytesIO(content))
    for para in doc.paragraphs:
        st.write(para.text)


# 读取并显示 PDF 文件内容
def display_pdf_content(content):
    pdf_document = fitz.open(stream=content, filetype="pdf")
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        page_text = page.get_text("text")
        st.write(page_text)


# Streamlit 应用

st.sidebar.title('File Operations')

operation = st.sidebar.selectbox('Choose Operation', ['View Files', 'Upload File'])

folder_path = st.text_input("Enter the S3 folder path:", "")

if folder_path:
    if operation == 'View Files':
        st.header('View and Download Files')
        files = list_files_in_bucket(BUCKET_NAME, folder_path)
        file_options = [file['Key'] for file in files]

        if file_options:
            selected_file = st.selectbox('Select a file', file_options)

            if selected_file:
                st.write(f'Selected file: {selected_file}')

                preview_clicked = st.button('Preview File')
                download_clicked = st.button('Download File')

                if preview_clicked:
                    content = download_file(BUCKET_NAME, selected_file)
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
                    st.download_button(
                        label='Download file',
                        data=content,
                        file_name=selected_file.split('/')[-1]
                    )
        else:
            st.write("No files found in the specified folder.")


    elif operation == 'Upload File':

        st.header('Upload a File')
        uploaded_file = st.file_uploader('Choose a file', type=['csv', 'pdf', 'docx'])

        if uploaded_file is not None:
            content = uploaded_file.read()
            upload_file(BUCKET_NAME, uploaded_file, folder_path)

            # 预览上传的文件
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
