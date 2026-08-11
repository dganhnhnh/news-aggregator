# Frontend

Chỗ trống dành cho Next.js frontend. Chưa scaffold. Khi cần bắt đầu, chạy từ thư
mục gốc repo:

```bash
pnpm create next-app@latest frontend --ts --app --tailwind --eslint --src-dir --import-alias "@/*"
```

API backend chạy ở `http://localhost:8000` (xem [../README.md](../README.md)).
`CORS_ALLOW_ORIGINS` trong `.env` cần bao gồm origin dev của frontend (mặc định
`http://localhost:3000`).
