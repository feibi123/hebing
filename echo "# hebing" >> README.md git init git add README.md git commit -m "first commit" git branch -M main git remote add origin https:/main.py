import os
import zipfile
import pandas as pd
import requests
import chardet
from flask import Flask, request, send_file, jsonify

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
MERGED_FILE = os.path.join(UPLOAD_FOLDER, "merged.csv")
EXTRACTED_FOLDER = os.path.join(UPLOAD_FOLDER, "extracted")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXTRACTED_FOLDER, exist_ok=True)


def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
    result = chardet.detect(raw_data)
    return result['encoding'] or 'utf-8'


@app.route("/")
def index():
    return '''
    <!doctype html>
    <title>CSV 合并工具</title>
    <h2>CSV 合并工具</h2>
    <form action="/upload" method=post enctype=multipart/form-data>
      <p>上传 ZIP 文件：<input type=file name=file>
         <input type=submit value=上传>
    </form>
    '''


@app.route("/upload", methods=["POST"])
def upload():
    zip_path = os.path.join(UPLOAD_FOLDER, "uploaded.zip")

    # 支持 URL 上传和表单上传
    if request.is_json and "file" in request.json:
        file_url = request.json["file"]
        try:
            r = requests.get(file_url)
            with open(zip_path, "wb") as f:
                f.write(r.content)
        except Exception as e:
            return f"无法下载远程文件：{e}", 400
    elif "file" in request.files:
        file = request.files["file"]
        if file.filename == "":
            return "未选择文件"
        file.save(zip_path)
    else:
        return "没有收到 zip 文件或 URL", 400

    # 清空旧的解压内容
    for root, dirs, files in os.walk(EXTRACTED_FOLDER):
        for f in files:
            os.remove(os.path.join(root, f))

    # 解压 ZIP，处理中文文件名乱码
    extracted_files = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for info in zip_ref.infolist():
            try:
                fixed_name = info.filename.encode('cp437').decode('gbk')
            except:
                fixed_name = info.filename.encode('cp437').decode('utf-8', errors='ignore')

            if info.is_dir():
                continue

            file_path = os.path.join(EXTRACTED_FOLDER, fixed_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(zip_ref.read(info.filename))
            extracted_files.append(file_path)

    # 合并 CSV
    all_dfs = []
    for file_path in extracted_files:
        if file_path.endswith(".csv"):
            try:
                encoding = detect_encoding(file_path)
                df = pd.read_csv(file_path, encoding=encoding)
                base_name = os.path.basename(file_path)
                df["日期"] = os.path.splitext(base_name)[0]  # 去掉 
                all_dfs.append(df)
            except Exception as e:
                print(f"跳过 {file_path}: {e}")

    if not all_dfs:
        return "没有有效的 CSV 文件"

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.to_csv(MERGED_FILE, index=False, encoding='utf-8-sig')

    download_link = "https://0ad1dfe7-6983-4a55-95bc-43047575460f-00-1012nv4ktkyyx.pike.replit.dev/download"

    return jsonify({"message": "CSV 合并完成", "download_url": download_link})


@app.route("/download")
def download():
    if os.path.exists(MERGED_FILE):
        return send_file(MERGED_FILE, as_attachment=True)
    return "CSV 文件不存在", 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
