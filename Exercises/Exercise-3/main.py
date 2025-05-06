import boto3
import gzip
import io

def download_from_s3(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body']  # Đây là một StreamingBody

def main():
    # Bước 1: Tải file .gz đầu tiên từ S3
    bucket = "commoncrawl"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
    print("Downloading paths.gz...")
    body = download_from_s3(bucket, key)

    # Bước 2: Giải nén trong bộ nhớ và đọc dòng đầu tiên
    with gzip.GzipFile(fileobj=body) as gz:
        with io.TextIOWrapper(gz, encoding='utf-8') as reader:
            first_path = reader.readline().strip()

    print(f"First WET path: {first_path}")

    # Bước 3: Tải file .wet.gz tương ứng từ S3
    print("Downloading WET file...")
    wet_body = download_from_s3(bucket, first_path)

    # Bước 4: Giải nén và in từng dòng
    print("Streaming and printing content:")
    with gzip.GzipFile(fileobj=wet_body) as wet_file:
        with io.TextIOWrapper(wet_file, encoding='utf-8') as reader:
            for line in reader:
                print(line.strip())

if __name__ == "__main__":
    main()
