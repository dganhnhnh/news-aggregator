# News Radar frontend

Next.js frontend cho News Radar. Browser gọi các route `/api/*` cùng origin;
Next.js server proxy các request này tới FastAPI.

Tạo file cấu hình local:

```bash
cp .env.example .env.local
```

`API_URL` mặc định là `http://localhost:8000` và chỉ được dùng phía server.
`ADMIN_API_URL` cũng chỉ dùng phía server, mặc định là
`http://localhost:8001`, và trỏ tới process `news-admin-api` cho trang quản lý
feed.

Khởi động frontend:

```bash
pnpm install
pnpm dev
```

Frontend mặc định chạy tại `http://localhost:3000`. API backend cần được khởi
động riêng. Trang `/feeds` cần cả `news-api` và `news-admin-api`; xem
[README chính](../README.md).
