from trafilatura import fetch_url, extract
downloaded = fetch_url("https://xusheng.dev/posts/reversing/mspaint_invisible_watermark/main")
print(extract(downloaded))
'Erin Spiceland is a software engineer and ...'
print(extract(downloaded, output_format="json", with_metadata=True))
