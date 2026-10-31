# DNSE Python SDK: hướng dẫn cho agent trong dự án này

Đây là ghi chú bổ sung của dự án, không phải tài liệu phát hành của DNSE. Khi làm
việc với SDK, đọc `README.md` trước, rồi đối chiếu chữ ký hàm và mô hình dữ liệu
trong `dnse/`. Dùng ví dụ trong `marketdata-api/` và `websocket-marketdata/` làm
mẫu cho các lời gọi tương ứng. Không suy đoán tên hàm hoặc trường dữ liệu.

## Cấu trúc và API cần dùng

- `dnse/api/client.py`: `DNSEClient` cho REST. Các hàm liên quan đến dashboard gồm
  `get_security_definition`, `get_latest_trade`, `get_ohlc`, `get_market_index`.
- `dnse/websocket/client.py`: `TradingClient` cho dữ liệu thời gian thực. Gọi
  `connect()` trước khi subscribe và `disconnect()` khi dừng. Các subscribe cho
  dashboard gồm `subscribe_quotes`, `subscribe_trade_extra`, `subscribe_sec_def`,
  `subscribe_ohlc`, `subscribe_ohlc_closed`, `subscribe_market_index`,
  `subscribe_estimated_market_index`, `subscribe_market_index_influence`.
- `dnse/websocket/models.py`: định nghĩa các đối tượng trả về. Kiểm tra kiểu và
  đơn vị từng trường tại đây; `receivedAt` là thời điểm nhận, khác thời điểm
  giao dịch do sàn cung cấp.
- `websocket-marketdata/`: ví dụ subscribe riêng cho từng loại sự kiện.

## Quy tắc tích hợp với v1

- Dùng API key và secret từ biến môi trường `DNSE_API_KEY`, `DNSE_API_SECRET`;
  không ghi khóa, token hoặc payload chứa khóa vào mã nguồn, giao diện hay log.
- Với WebSocket market data, dùng `wss://` và kiểm tra `board_id`, `symbol`,
  `resolution` theo ví dụ và chữ ký hàm thực tế. Nến phái sinh của DNSE được
  subscribe bằng `symbolType` (ví dụ `VN30F1M`), khác mã hợp đồng đang giao dịch.
- Chuẩn hóa giá, khối lượng, thời gian và chiều mua/bán trong `v1/dnse_normalize.py`
  trước khi gọi `EventSink`. Không để object của SDK đi thẳng vào lưu trữ hoặc UI.
- Trong chế độ SSI + DNSE, chỉ phát kênh thuộc DNSE hoặc kênh SSI đang thiếu;
  kiểm tra định tuyến ở `v1/hybrid_provider.py` để tránh ghi hai nguồn vào cùng
  một box. Đóng góp chỉ số và VN30 ước tính lấy từ DNSE.
- Xử lý mất kết nối, đăng ký thất bại và lỗi dữ liệu mà không để một nguồn làm
  dừng nguồn còn lại. Khi sửa adapter, chạy
  `.venv/bin/python -m unittest discover -s v1 -p 'test_*.py'` từ gốc dự án.

Dashboard chỉ đọc dữ liệu thị trường. Các ví dụ đặt/sửa/hủy lệnh trong
`trading-api/` và `examples/` không phải bài kiểm tra cho adapter dữ liệu;
không chạy chúng khi kiểm tra thay đổi dashboard.
