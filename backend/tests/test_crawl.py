from trafilatura import fetch_url, extract, extract_metadata
downloaded = fetch_url("https://xusheng.dev/posts/reversing/mspaint_invisible_watermark/main")
print(extract_metadata(downloaded))
# print(extract(downloaded, output_format="json", with_metadata=True))
