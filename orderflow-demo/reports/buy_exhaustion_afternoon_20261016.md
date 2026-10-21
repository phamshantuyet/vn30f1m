# Kiểm thử BUY suy yếu — 41I1GB000, 16/10/2026

Kết quả: công thức khớp bộ kiểm tra độc lập, nhưng phiên chiều này chưa chứng minh được khả năng dự báo giá giảm tiếp. Không chỉnh ngưỡng sau khi xem kết quả phiên chiều.

## Công thức đã cố định

Ở mỗi giây t, dùng dữ liệu nguồn có timestamp ≤t:

```text
B10 = BUY trong (t−10s, t]
S10 = SELL trong (t−10s, t]
B30_trước = BUY trong (t−40s, t−10s]
R_buy = (B10 / 10) / (B30_trước / 30)
SellShare = S10 / (B10 + S10)
Mid = (Best Bid + Best Ask) / 2

Cảnh báo khi đồng thời:
R_buy < 0,5
SellShare > 65%
Mid(t) − Mid(t−10s) ≤ −0,1 điểm
Best Bid(t) < Best Bid(t−10s)
Best Ask(t) < Best Ask(t−10s)
```

Giá nội bộ dùng tick 0,1 điểm; so sánh ngưỡng bằng số nguyên để giữ đúng biên: R_buy=0,5 và SellShare=65% không kích hoạt; giảm Mid đúng 0,1 điểm được tính. UNKNOWN không góp vào BUY/SELL. Đây là cảnh báo suy yếu, không phải nhận dạng ý định hay trạng thái vốn của người mua.

Điều kiện dữ liệu: khởi động đủ 40s, ít nhất 20 bản ghi khớp và 50 HĐ có chiều trong 10s; tỷ lệ có chiều ≥90%; baseline BUY >0; Tape cuối ≤5s; Quote hợp lệ tại t và t−10s không quá 5s; độ phủ Quote hợp lệ ≥80% cửa sổ 10s. Quote giao nhau/thiếu phía bị loại.

## Cách kiểm thử phiên chiều

Khoảng kiểm tra 13:00:00–14:45:00, theo dữ liệu ghi nhận; các đoạn thiếu giao dịch và Quote bị chặn bằng điều kiện dữ liệu. Engine khởi động từ phiên chiều, không mang dữ liệu sáng qua giờ nghỉ. Mỗi giây chỉ phát cảnh báo từ quá khứ; lưu toàn bộ dự đoán trước khi tính giá tương lai.

Kết quả tương lai đo bằng Mid tại mốc 1/3/5 phút trừ Mid lúc cảnh báo, nhằm giảm nhiễu giá khớp nhảy giữa Bid/Ask. Mỗi đích phải có Quote hợp lệ và Tape không quá 5s; đích vượt cuối file không chấm. Đây không phải lãi/lỗ giao dịch có thể thực thi.

- 6,301 giây kiểm tra; 3,653 giây đủ điều kiện.
- 444 giây có cảnh báo, gộp thành 108 đợt (chỉ lấy giây đầu mỗi đợt).
- Giữ đợt đầu rồi đợt kế tiếp cách ≥5 phút: 16 mẫu, chọn trước khi xét kết quả tương lai. Không khẳng định các mẫu này độc lập thống kê.

| Sau cảnh báo | Mẫu cách ≥5 phút | Giá thấp hơn | Không đổi | Giá cao hơn | ΔMid trung bình |
|---|---:|---:|---:|---:|---:|
| 1 phút | 16 | 6/16 (37.50%) | 0 | 10 | +0.059 điểm |
| 3 phút | 16 | 8/16 (50.00%) | 1 | 7 | -0.081 điểm |
| 5 phút | 16 | 7/16 (43.75%) | 2 | 7 | -0.113 điểm |

Nếu đếm tất cả đợt cảnh báo, còn có chồng lấn cửa sổ:

| Sau cảnh báo | Mẫu đủ dữ liệu | Tỷ lệ thấp hơn | ΔMid trung bình |
|---|---:|---:|---:|
| 1 phút | 107 | 45.79% | -0.005 điểm |
| 3 phút | 103 | 56.31% | -0.014 điểm |
| 5 phút | 103 | 50.49% | +0.022 điểm |

Baseline tham khảo: các mốc không cảnh báo, đủ dữ liệu trên lưới 5 phút bắt đầu 13:01. Chỉ 11 mốc (10 mốc ở 5 phút), chưa đủ để so sánh lợi thế thống kê; số liệu đầy đủ trong summary.json.

## Kiểm tra độc lập

- `buy_exhaustion.py`: công thức thuần và engine streaming dùng prefix/bisect, giá tick nguyên.
- `verify_buy_exhaustion_raw.py`: đọc lại JSONL gốc; dùng datetime/Decimal riêng, SQL khoảng thời gian và Fraction để tính lại. Không import công thức, cấu hình, engine, cache adapter hoặc hàm tổng hợp của production.
- `compare_buy_exhaustion.py`: chỉ đọc hai bộ CSV để đối chiếu; không import hai bên triển khai.
- `test_buy_exhaustion.py`: ví dụ số tính tay, các biên nghiêm ngặt, thiếu dữ liệu và append dữ liệu tương lai.
- `test_buy_exhaustion_comparison.py`: cố ý làm sai cảnh báo, khối lượng, hướng Quote và bỏ dòng để xác nhận bộ đối chiếu phát hiện sai lệch.

Đã đối chiếu 6,301 dòng × 25 trường và 324 nhãn giá tương lai của 108 đợt; sai khác: **0**. Toàn bộ bộ test Python: **20 tests PASS**.

## Tái lập

Chạy từ thư mục dự án:

```bash
python3 -m unittest discover -s orderflow-demo -p "test_*.py"
python3 orderflow-demo/backtest_buy_exhaustion.py
python3 orderflow-demo/verify_buy_exhaustion_raw.py
python3 orderflow-demo/compare_buy_exhaustion.py
```

## Giới hạn

Chỉ một phiên chiều. Các ngưỡng xuất phát từ ví dụ buổi sáng; dữ liệu cùng ngày đã được khảo sát ở các phân tích trước, nên đây không phải thử nghiệm mù trên ngày hoàn toàn chưa xem. Cần dữ liệu nhiều ngày mới có thể đánh giá độ bền. Không có receive timestamp/sequence nguồn nên dùng source time; không suy ra nhân quả giữa hai feed ở cùng mili giây. Không mô phỏng phí, độ trễ, trượt giá hay khả năng khớp lệnh. Công thức chưa được nối vào box đánh giá HTML hiện tại.
