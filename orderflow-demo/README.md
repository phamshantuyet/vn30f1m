# VN30F1M Order Flow — bản demo HTML

Mở `dist/index.html` trong trình duyệt. HTML, CSS và JavaScript nằm trong cùng một file; không cần cài dependency hoặc kết nối internet để chạy demo.

Dữ liệu hoàn toàn mô phỏng, từ 09:30 đến 10:30 ngày 13/10/2026, với mã hợp đồng `DEMO-VN30-2610`. Không kết nối DNSE và không dùng làm dữ liệu giao dịch.

## Điều khiển

- Demo / Replay, phát / tạm dừng, tiến 1 giây, tua thời gian, tốc độ 1×–10×.
- Nến 1m / 3m / 5m; Heatmap 1s / 5s / 15s; cửa sổ 5 / 15 / 30 / 60 phút.
- Cuộn chuột hoặc nút +/− để zoom trục giá; kéo biểu đồ để dịch vùng giá; nút tâm ngắm bật lại theo giá.
- Bật/tắt Imbalance, Trade Bubbles, Liquidity Wall, Session POC, Volume và CVD.
- Cài đặt: Linear / Logarithmic, thang màu tương đối, đoạn mất Quote, tải JSON cửa sổ đang xem.
- Di chuột trên biểu đồ để xem mức giá/thời gian và thông tin liên quan. Phím Space phát/tạm dừng; Escape đóng menu.

## Dữ liệu và giới hạn demo

Các panel dùng chung tape và book mô phỏng. Footprint/Delta/CVD/Profile lấy từ tape; DOM lấy từ Quote. Heatmap tích phân Snapshot theo khoảng một giây, tổng hợp bucket có trọng số thời gian; không cộng các quantity Snapshot để coi là volume giao dịch. Các mức ngoài vùng book không được gán 0.

Đoạn 10:22:35–10:22:53 mô phỏng Quote UNKNOWN trong khi Tape vẫn hoạt động. Giao dịch UNKNOWN được tính vào Volume/Profile, không tính vào Delta/CVD. Giữ nguyên khoảng chưa quan sát khi Replay; chỉ hiển thị dữ liệu đến thời điểm đã chọn.

Đây là prototype trình bày và tương tác. Tính toán demo trong trình duyệt để file chạy độc lập; hệ thống thực tế theo `1.md` sẽ chuyển tính toán vào Backend, nhận Snapshot/Patch qua API/WebSocket. Liquidity Wall dùng ngưỡng demo, không phải thuật toán tín hiệu đã kiểm chứng. Tape demo đầy đủ về số giao dịch nhưng một phần chiều là UNKNOWN. Chưa triển khai backend, lưu Parquet, DNSE Adapter hoặc phục hồi WSS thực tế.

## Replay dữ liệu đã ghi — 41I1GB000

Mở `http://localhost:8095/orderflow-replay.html` khi đang chạy static server ở cổng 8095. Trang tự tải gói dữ liệu đã tính sẵn, phát từ đầu ngày ở tốc độ 60× và dừng ở cuối ngày. Có các tốc độ 1×–600×, tạm dừng, tua, bước một giây, về đầu phiên và cuối ngày. Các khung nến: 30s, 1m, 3m, 5m, 15m.

Nguồn mới nhất có cả Quote và Trade Tape: **16/10/2026, G1**, từ 08:45:00.683 đến 14:45:00.701. Có **732.140 Quote**, **85.257 giao dịch**, tổng **190.945 HĐ** (BUY 95.905, SELL 95.038, UNKNOWN 2). Đối chiếu tổng match quantity với totalVolumeTraded cuối ngày bằng nhau.

Gói tĩnh được Python tính trước từ mọi bản ghi. Trình duyệt áp dụng các patch đã tính sẵn theo bước một giây; không lấy dữ liệu tương lai khi tua lại. Các đỉnh Bid/Ask tính cả Quote giữa hai mốc hiển thị. Dấu — trong Bid/Ask hiện tại chỉ vùng chưa quan sát được, không phải 0. Quote thiếu một phía hoặc Bid ≥ Ask được ẩn khỏi cột hiện tại. Ngoài khoảng nghỉ cấu hình, Quote quá 30 giây cũng bị ẩn. Khoảng 11:30–13:00 (UTC+7, theo yêu cầu người dùng), giữ sổ lệnh hợp lệ cuối cùng cùng Bid/Ask Depth và OBI, gắn nhãn dữ liệu cũ và thời điểm Quote. Nếu cả Tape và Quote không có cập nhật hơn 30 giây, vùng biểu đồ neo ở bản ghi thị trường cuối cùng. Đồng hồ Replay vẫn chạy, không tạo giao dịch giả. Đánh giá xu hướng tạm dừng trong khoảng nghỉ; có bản ghi mới thì vẫn cập nhật, và từ 13:00 áp dụng lại kiểm tra độ mới bình thường.

Dữ liệu này chỉ có timestamp nguồn, không có thời gian nhận hay trade ID. Replay sắp theo timestamp nguồn; nếu trùng timestamp thì giữ thứ tự ngược với thứ tự dòng trong từng file (file gốc lưu mới nhất trước). Không khẳng định thứ tự giữa hai nguồn ở cùng timestamp, khả năng chống trùng tuyệt đối hoặc tính đầy đủ của feed. Các mảng bid/offer được dùng như ảnh sổ lệnh đã ghi; chưa xác minh cam kết delivery của nguồn. Không ghi đè hoặc sửa raw data.

Tạo lại cache và gói tĩnh, tự chọn ngày mới nhất có cả hai nguồn:

```bash
python3 orderflow-demo/export_replay.py
python3 -m http.server 8095 --directory orderflow-demo/dist
```

Có thể chạy backend truy vấn timestamp đến mili giây, tính tổng hợp bằng Python khi xem:

```bash
python3 orderflow-demo/replay_server.py --port 8096
```

Mở `http://localhost:8096/orderflow-replay.html`. Trang tự dùng `/api/meta` và `/api/state` nếu có backend; nếu không có sẽ tải gói tĩnh tại `dist/replay-data/`. Cache SQLite nằm trong `.replay-cache/`, có thể tạo lại từ raw.

Kiểm tra logic Replay:

```bash
python3 -m unittest discover -s orderflow-demo -p test_replay.py
node orderflow-demo/test_static_replay.cjs
```

## Đánh giá dòng lệnh cuối trang

`flow_assessment.py` tính nhận xét ở từng mốc Replay bằng dữ liệu nguồn đã có tại mốc đó. Cửa sổ cố định 30s/60s/180s độc lập khung nến; kết hợp lệch BUY/SELL, giá khớp đầu/cuối, dịch chuyển Best Bid/Ask và cấu trúc đỉnh/đáy của ba đoạn thời gian. OBI 3 trung bình theo thời gian và OFI xấp xỉ bổ sung bằng chứng, không được cộng vào Delta hoặc coi là khối lượng giao dịch.

Kết luận chung có hướng khi cả ba cửa sổ cùng xác nhận. Trường hợp lực chủ động không tạo được tiến triển giá có thể được gắn ứng viên hấp thụ; không suy ra đảo chiều hoặc iceberg. Chặn kết luận khi lịch sử chưa đủ, Tape quá ít/quá cũ, tỷ lệ UNKNOWN cao hoặc Quote quá hạn/thiếu/giao nhau. Ngưỡng và phiên bản quản lý tập trung trong `AssessmentConfig`; các quy tắc chưa được kiểm định khả năng dự báo. Chưa có dữ liệu trạng thái phiên đầy đủ để đánh giá phiên định kỳ.

Gói tĩnh cũng được Python tính trước phần đánh giá; trình duyệt chỉ hiển thị kết quả ở mốc đã chọn. Sau thay đổi quy tắc, chạy lại `export_replay.py`. Unit test:

```bash
python3 -m unittest discover -s orderflow-demo -p 'test_*.py'
```

## Box giao dịch lớn

Cuối trang Replay có danh sách từng bản ghi Trade Tape **BUY/SELL có match quantity lớn hơn ngưỡng cấu hình**, mặc định >50 HĐ, giá khớp và timestamp nguồn đến mili giây (UTC+7). Ngưỡng nhận số nguyên không âm, lưu trong localStorage; nhập 0 để xem tất cả BUY/SELL. Bảng phân trang 100 dòng, số tổng tính trên toàn bộ kết quả theo ngưỡng đến mốc Replay. Lọc BUY/SELL, hiển thị mới nhất trước và chỉ hiện giao dịch đến mốc Replay; tua ngược sẽ loại các giao dịch tương lai. Các số tổng trên box tính trong tập giao dịch lớn đã xuất hiện, không phải toàn bộ Tape. Không gộp nhiều khớp nhỏ và không suy ra kích thước lệnh gốc. Ngày 16/10/2026 có 73 bản ghi thỏa điều kiện: 36 BUY, 37 SELL.

Tạo lại riêng danh sách bằng `python3 orderflow-demo/export_large_trades.py`; `export_replay.py` cũng tự xuất danh sách này. Backend API cung cấp cùng dữ liệu trong trường `large_trades`, nhận tham số `large_min` (mặc định 50). File tĩnh lưu tất cả BUY/SELL để hỗ trợ cả ngưỡng nhỏ hơn 50; chỉ lọc theo ngưỡng và mốc Replay khi hiển thị.

## Công thức BUY suy yếu và kiểm tra độc lập

`buy_exhaustion.py` là module riêng, giữ nguyên quy tắc `R_buy <0,5`, `SellShare >65%`, Mid giảm ít nhất 0,1 điểm và cả Best Bid/Ask hạ trong 10s. Baseline BUY dùng 30s liền trước cửa sổ 10s. Công thức mới chưa được nối vào box đánh giá HTML.

Kiểm thử phiên chiều 16/10/2026: `backtest_buy_exhaustion.py` lưu dự đoán từng giây trước khi tính kết quả giá sau 1/3/5 phút. `verify_buy_exhaustion_raw.py` tính lại bằng JSONL gốc, SQL và Fraction; không import code/config/bộ đọc dữ liệu của công thức. `compare_buy_exhaustion.py` đối chiếu hai bộ kết quả. Bộ unit test có ví dụ tính tay, kiểm tra biên, dữ liệu thiếu, chống dùng tương lai và cố ý sửa sai kết quả để kiểm tra khả năng phát hiện lỗi.

```bash
python3 -m unittest discover -s orderflow-demo -p 'test_*.py'
python3 orderflow-demo/backtest_buy_exhaustion.py
python3 orderflow-demo/verify_buy_exhaustion_raw.py
python3 orderflow-demo/compare_buy_exhaustion.py
```

Kết quả và giới hạn: [Báo cáo phiên chiều](reports/buy_exhaustion_afternoon_20261016.md). Hai bộ tính khớp 6.301 mốc và 324 nhãn giá tương lai, nhưng tỷ lệ giảm tiếp trên 16 đợt cách nhau ≥5 phút chỉ 37,5% / 50% / 43,75% ở 1m/3m/5m. Tính đúng công thức không đồng nghĩa đã có khả năng dự báo tốt.

## Thử nghiệm khối lượng và tốc độ BUY

`buy_speed.py` đo riêng khối lượng BUY/giây và số lượt khớp BUY/giây ở cửa sổ 5/10/30s, so với 3 cửa sổ liền trước. Ngưỡng suy yếu cố định: tỷ lệ <0,5; biến thể kết hợp yêu cầu cả hai tỷ lệ <0,5. Không suy ra số lệnh gốc từ số lượt khớp. Các biến thể và công thức đầy đủ dùng chung bộ lọc chất lượng v1 và thời gian khởi động 120s.

```bash
python3 orderflow-demo/backtest_buy_speed.py
python3 orderflow-demo/verify_buy_speed_raw.py
python3 -m unittest discover -s orderflow-demo -p 'test_*.py'
```

[Báo cáo BUY volume/speed](reports/buy_speed_afternoon_20261016.md) có cả kết quả toàn bộ đợt tín hiệu và mẫu giãn cách ≥5 phút; kết quả thay đổi rõ theo cách lấy mẫu. Đây là thử nghiệm bổ sung sau khi đã xem phiên chiều, không phải dữ liệu kiểm định mới. Script kiểm tra độc lập đọc Tape JSONL gốc, dùng SQL SUM/COUNT, không import công thức hoặc cache; dùng lại oracle Quote độc lập trước đó cho chất lượng dữ liệu và nhãn giá tương lai. Chưa nối các biến thể vào HTML.

## DNSE realtime + lưu lịch sử trong phiên

Backend dùng `dnse-sdk-openapi` để nhận **Trade Extra + Quote**, đẩy trạng thái LIVE lên giao diện bằng **WebSocket** cùng cổng web. Trang riêng `orderflow-live.html` dùng giao diện của Replay; các bản demo/Replay cũ vẫn chạy như trước.

Trang LIVE/Replay hiển thị tốc độ khớp chủ động BUY/SELL và chênh lệch BUY−SELL (HĐ/giây) trong cửa sổ lùi 10s và 60s, tính từ Trade Extra đến đúng mốc đang xem. Tổng BUY/SELL/UNKNOWN và số lượt khớp đi kèm; không coi khối lượng Quote đang chờ là giao dịch. Nếu cửa sổ chưa đủ lịch sử, giao với đoạn mất kết nối, hoặc nguồn LIVE ngắt/quá 30s không có dữ liệu, tốc độ hiển thị `—` thay vì 0. Trong giờ nghỉ cũng tạm ẩn tốc độ.

Đầu trang đổi nhãn LIVE/REPLAY đúng theo chế độ xem và hiển thị tuổi dữ liệu DNSE WSS cùng tuổi bản tin WebSocket đã nhận trên màn hình. Tua thời gian, bấm phát/tạm dừng hoặc phân trang lệnh lớn sẽ rời chế độ LIVE; bấm **● LIVE** để theo lại dữ liệu mới.

Sổ lệnh giữ đỉnh BID/ASK từng giá và ghi lần gần nhất một giá rời vùng Quote quan sát được rồi xuất hiện lại. Dòng nhỏ `↩ 500→400` dưới MAX nghĩa là lần thấy cuối trước khi rời vùng có 500 HĐ, lần đầu thấy lại có 400 HĐ; MAX vẫn là đỉnh của cả phiên. Trường hợp `500→600` sẽ cập nhật MAX lên 600. Bản ghi thiếu một phía không được coi là giá biến mất; sự vắng mặt trong vùng quan sát không được coi là khối lượng 0. Số lần xuất hiện lại và thời điểm có trong tooltip; Replay chỉ dùng các Quote đến mốc đang xem. Quote đến trễ làm mới lịch sử sổ lệnh của cache LIVE theo thứ tự thời gian nguồn, không dựng lại Trade Tape.

`SỐ MỨC GIÁ QUOTE` đếm giá khác nhau từng xuất hiện ở Bid hoặc Ask. `SỐ MỨC GIÁ KHỚP (TAPE)` đếm giá khớp khác nhau trên Trade Extra; giao dịch lặp lại cùng giá chỉ tính một lần. Cả hai số tính từ đầu dữ liệu đã thu đến mốc LIVE/Replay đang xem, không nội suy các mức giá chưa quan sát.

Cài dependency và chạy từ thư mục gốc dự án:

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python orderflow-demo/live_server.py --port 8097
```

Hoặc chạy `bash orderflow-demo/run_live.sh --port 8097`.

Mở `http://localhost:8097/orderflow-live.html`. Backend đọc `.env` ở thư mục gốc, không phụ thuộc thư mục hiện hành; biến môi trường của process có ưu tiên cao hơn `.env`:

Để đo độ trễ theo từng chặng, mở `http://localhost:8097/dom-latency.html`. Trang DOM dạng thang giá nhận Quote/Trade Extra từ `/api/ws/raw`: bản ghi được gửi từ cache RAM ngay sau khi SDK callback xử lý, không đợi SQLite hoặc snapshot Order Flow. Box 2 cuộn qua mọi mức giá BID/ASK đã xuất hiện trong Quote của phiên và giữ thêm 61 mức quanh best bid/ask hiện tại; các khoảng chưa ghi nhận được thu gọn. Nút “Về giá hiện tại” đưa thang giá về best BID/ASK mà không ép cuộn khi người dùng đang xem lịch sử. DOM hiển thị BID/ASK hiện tại, MAX theo Quote từ đầu phiên (nạp từ dữ liệu đã lưu và cập nhật realtime), depth, spread và OBI. Khi một giá rời 10 mức Quote hiện tại, khối lượng BID/ASK cuối cùng ghi nhận ở giá đó vẫn hiện màu xám nhạt; đó là dữ liệu lịch sử, không được cộng vào depth/OBI hiện tại. Giá trị cuối cùng này cũng được khôi phục từ Quote đã lưu khi tải lại trang. Box 5 còn hiển thị sáu trường `totalVolumeTraded`, `grossTradeAmount`, `avgPrice`, `highestPrice`, `lowestPrice`, `openPrice` từ bản Trade Extra mới nhất theo thời gian nguồn; bộ giá trị cuối được giữ trong metadata SQLite để hiện lại sau khi tải trang/khởi động lại server. Trang cũng hiển thị thời điểm nguồn, SDK nhận, cache RAM cập nhật, server gửi và trình duyệt nhận; cùng số Quote đến chung một mili giây và các đợt WebSocket. Nếu trình duyệt ở máy khác, phép trừ thời gian server→trình duyệt phụ thuộc đồng hồ hai máy; đồng hồ nguồn DNSE cũng có thể lệch với máy chạy server. Luồng chẩn đoán chỉ giữ tối đa 8192 bản ghi mới trong RAM; nếu trang đọc không kịp, ô bản tin vượt bộ đệm sẽ tăng.

Trong cột GIÁ của Box 2, dấu `^` ở mé BID nghĩa là Trade Extra `SELL` đã khớp tại giá BID đó; dấu `▼` ở mé ASK nghĩa là `BUY` đã khớp tại giá ASK đó. Dấu chỉ hiện ở phía giá từng có Quote trong phiên, được giữ khi mức giá rời sổ hiện tại và khôi phục từ Trade đã lưu khi tải lại. Trade có chiều `UNKNOWN` không được gán vào phía nào. Dấu phản ánh có ít nhất một khớp tại giá đó trong phiên đã thu, không phải khối lượng lệnh đang chờ.

Box 13 nằm ngay trên Box 4 hiển thị giá khớp hiện tại và chênh lệch theo điểm/phần trăm so với giá tham chiếu và giá mở cửa. Giá tham chiếu lấy từ `basicPrice` của kênh WSS `security_definition.G1.msgpack` cho đúng mã hợp đồng; giá mở cửa lấy từ `openPrice` của Trade Extra. Giá tham chiếu được lưu trong metadata SQLite của ngày và khôi phục khi tải lại; thiếu nguồn nào thì chênh lệch tương ứng hiện `—`, không suy ra từ giá khác.

Backend cũng đăng ký hai kênh WSS `ohlc.1.msgpack` và `ohlc_closed.1.msgpack` bằng `symbolType` phái sinh (mặc định `VN30F1M`), theo yêu cầu của DNSE. Mã này có thể đổi bằng `--ohlc-symbol-type` hoặc `DNSE_OHLC_SYMBOL_TYPE` khi theo dõi hợp đồng khác; nó phải tương ứng với mã hợp đồng `--symbol` để Box 14 hiển thị đúng nến. Nến 1 phút đang hình thành được cập nhật trong cache RAM và bảng SQLite `ohlcv_1m`; bản đã đóng thay thế bản đang hình thành cùng phút và không bị bản đang hình thành đến trễ ghi đè. `GET /api/ohlcv-1m` trả toàn bộ nến đã nhận trong ngày, gồm OHLC, volume, thời điểm cập nhật/nhận và cờ `closed`. Cache RAM phục vụ tính toán nhanh; SQLite giúp khôi phục sau khi khởi động lại. Bảng này không được đưa vào backup và bị xóa cùng file LIVE cũ khi qua ngày. Chỉ có nến nhận được kể từ lúc kết nối WSS, không tự tải bù lịch sử trước đó.

Box 14 ngay dưới Box 13 theo dõi nến 1 phút mới nhất qua `/api/ws/raw`. Khi nến **chưa đóng** và volume **>1.500 HĐ**, nền box chuyển đỏ, số volume phóng lớn và màu nền/viền nhấp nháy; nếu trình duyệt bật chế độ giảm chuyển động, hiệu ứng dừng nhưng cảnh báo đỏ vẫn hiện. Đúng 1.500 chưa cảnh báo. Nến đóng hoặc nến mới đến sẽ cập nhật trạng thái, và bản nến chưa đóng đến trễ không thể mở lại nến đã đóng. Khi tải lại trang, box lấy nến mới nhất từ cache SQLite đã khôi phục.
Box 14 còn đếm ngược số giây đến hết phút của nến đang hình thành, dựa trên mốc bắt đầu nến và đồng hồ server được đồng bộ qua `/api/ws/raw`. Hết giờ mà chưa nhận bản tin nến đóng thì hiển thị `0 s · chờ xác nhận`; chỉ bản tin `ohlc_closed` mới đổi sang `Đã đóng`.

Box 15 ngay trên Box 1 liệt kê các nến 1 phút đã thu trong phiên có volume >1.500 HĐ, mới nhất trước. Bảng tải lịch sử từ `/api/ohlcv-1m` và cập nhật bằng `/api/ws/raw`; nến chưa đóng được ghi rõ và thay bằng bản đã đóng cùng phút. Hai cột biến động dùng `giá đóng − giá mở` theo điểm và `(giá đóng − giá mở) / giá mở × 100%`, với dấu tăng/giảm. Nến không còn vượt ngưỡng sau bản cập nhật sẽ rời bảng. Lịch sử chỉ gồm những nến mà backend đã nhận trong ngày hiện tại; không tự tải bù trước lúc kết nối và không giữ OHLCV khi xóa file LIVE ngày cũ.

Box 16 ngay dưới Box 15 vẽ hai đường theo từng giây nguồn của Trade Extra: số bản tin khớp lệnh/giây và tổng `quantity` hợp đồng khớp/giây. Mỗi biểu đồ có thêm đường nét đứt EMA20 tính trên 20 mốc giây (`α = 2/21`), lấy giây có khớp đầu tiên làm giá trị khởi tạo và tính cả các giây không ghi nhận khớp là 0. EMA được tính trên toàn bộ lịch sử đã thu trước khi cắt khoảng xem, nên đổi từ 5 sang 15 hoặc 60 phút không làm đổi EMA của cùng một giây. Hai biểu đồ dùng trục giá trị riêng để không che mất đường số lệnh. Có thể xem 5 phút, 15 phút, 60 phút hoặc toàn bộ phiên đã thu; rê chuột lên biểu đồ để xem số chính xác và EMA20 của từng giây. `GET /api/trade-speed` trả các giây có khớp dưới dạng `[epoch_ms, số_lệnh, số_hợp_đồng]`, từ Trade đã nhận trong RAM và khôi phục từ SQLite sau khi khởi động lại. Trang ghép phần lịch sử với `/api/ws/raw` theo cursor để không đếm trùng trong lúc tải; các giây không ghi nhận khớp được vẽ bằng 0 trong phạm vi dữ liệu đã thu. Hai số lớn phía trên là tốc độ của giây hiện tại theo đồng hồ server.
Nút **Toàn màn hình** trên tiêu đề Box 16 mở rộng cả box và biểu đồ. Bấm **Thu nhỏ** hoặc phím Esc để quay lại; nếu trình duyệt không hỗ trợ Fullscreen API, trang dùng lớp phủ toàn màn hình với cùng thao tác đóng.

Box 17 dưới Box 16 lấy 30 mã VN30 từ [API nhóm VN30 của SSI](https://iboard-query.ssi.com.vn/stock/group/VN30) khi server khởi động. Danh sách hợp lệ gần nhất được lưu tại `data/live/vn30_symbols.json` để dùng khi SSI tạm thời không truy cập được; nếu chưa có cache thì dùng danh sách HOSE kỳ 7/2026 trong `vn30_stocks.py` làm dự phòng. Bấm vào mã để mở trang phân tích kỹ thuật của mã đó trên DNSE trong tab mới. Giá cổ phiếu tính theo nghìn đồng/cp. Giá tham chiếu lấy từ DNSE Security Definition `G1`; giá khớp, thấp nhất và cao nhất trong ngày lấy từ Trade Extra `G1`. Cột phần trăm là `(giá khớp mới nhất − giá tham chiếu) / giá tham chiếu × 100`; thiếu một trong hai giá thì để `—`. Bấm tiêu đề bất kỳ cột nào để sắp xếp tăng/giảm; cột số mặc định xếp giảm dần, mã mặc định xếp A–Z, giá thiếu luôn nằm cuối và thứ tự được giữ khi bảng cập nhật. Backend đăng ký WSS cho 30 mã để cập nhật liên tục và gọi REST DNSE lúc khởi động để lấy ảnh chụp giữa phiên. Snapshot mới nhất của mỗi mã được lưu vào SQLite và trả qua `GET /api/vn30-stocks`; trang đọc lại mỗi 2 giây.

Box 9 nằm ngay dưới box 5, nhận hai kênh DNSE SDK `market_index.VN30.msgpack` và `estimated_market_index.VN30.msgpack` rồi đẩy qua `/api/ws/index`. Box hiển thị giá VN30 chính thức, giá VN30 dự tính do DNSE cung cấp, biến động, độ rộng thị trường và thời điểm nguồn/nhận. Giá khớp cuối của hợp đồng đang theo dõi lấy từ `/api/ws/raw`; trình duyệt tính **basis = giá khớp cuối hợp đồng − giá trị VN30**, riêng cho VN30 chính thức và VN30 dự tính, mỗi khi một trong các giá đổi. Giá trị mới nhất của hai kênh VN30 được lưu trong bảng SQLite `market_index_latest` của phiên để hiện lại khi tải trang hoặc khởi động lại server; các bản tin được chấp nhận từ khi cập nhật chương trình còn được lưu vào `market_index_history`. Không tự tính chỉ số dự tính từ giá cổ phiếu thành phần.

Box 7 của `dom-latency.html` nhận kênh DNSE WSS `market_index_influence.VN30.1.msgpack` qua server; trường `influence` của từng mã là **điểm đóng góp DNSE tính sẵn**, dương kéo tăng, âm kéo giảm. Biểu đồ SVG chữ nhật có trục X theo giờ nhận tại server (UTC+7, nghỉ trưa thu gọn) và trục Y theo tổng điểm của các mã **có trong bản tin**; mã vắng mặt không được gán 0, tổng các mã top không nhất thiết bằng mức thay đổi VN30. Ô **Khung** bắt đầu ở 1 phút, tăng từng phút; mỗi cột lấy bản tin cuối trong khung đã chọn. Đổi khung gộp lại lịch sử một phút trên trình duyệt, không cần tải lại. Box cuộn ngang qua lịch sử đã thu, mở ở mốc mới nhất, có nút **Cũ hơn** và **Mới nhất**. `/api/ws/influence` đẩy mỗi bản tin mới ngay từ RAM và cập nhật cột của khung đang chạy; `/api/influence-history?bucket=60` trả ảnh chụp cuối mỗi phút để tải lại trang. SQLite cùng phiên lưu ảnh chụp cuối của từng giây vào bảng `index_influence` dưới dạng MessagePack nén khi có lợi; trước khi xóa file LIVE ngày cũ, bảng này được lưu sang file backup. Nếu mở trực tiếp file HTML (`file:`) không qua server, box 7 dùng 12 mã/186 mốc một phút giả lập để xem bố cục. Hệ thống không tự lấy bù dữ liệu trước lúc bắt đầu thu.

Box 8 bên dưới là **100% Stacked Area Chart** từ cùng các bản tin và khung thời gian của box 7. Ở mỗi mốc có dữ liệu khác 0, diện tích mỗi mã bằng `abs(influence) / sum(abs(influence) của các mã có trong bản tin) × 100%`; màu xanh cho ảnh hưởng dương, đỏ cho ảnh hưởng âm. Di chuột/chạm vào vùng màu để xem mã, điểm và tỷ trọng; cuộn ngang ở hai biểu đồ được đồng bộ. Mốc thiếu bản tin hoặc mọi ảnh hưởng bằng 0 để trống, không nội suy. Các mã vắng mặt không được hiểu là ảnh hưởng 0; tỷ trọng chỉ phản ánh nhóm mã DNSE đã gửi, không phải tỷ trọng toàn bộ VN30.

Box 10 ngay dưới box 8 là **Index Contribution Chart**, trình bày bản tin `market_index_influence.VN30.1.msgpack` mới nhất thành bảng ba cột `Mã | Âm | Dương`. Mỗi mã có một dòng, xếp theo điểm hiện tại từ cao xuống thấp; điểm âm và thanh đỏ nằm ở cột Âm, điểm dương và thanh xanh nằm ở cột Dương. Khi điểm hiện tại có dấu tương ứng, cột đó ghi `điểm hiện tại (cực trị cùng chiều đã ghi nhận trong phiên)`, ví dụ `-2.06 (-4.00)` hoặc `+2.06 (+4.00)`. Khi mã đổi dấu, cột phía bên kia vẫn hiện cực trị cũ trong ngoặc, không vẽ thanh; chẳng hạn VHM đang dương vẫn có thể hiện `(-2.43)` ở cột Âm. Cực trị được khôi phục từ `index_influence_extrema` khi tải lại trang. Thanh chỉ thể hiện điểm hiện tại, kéo dài từ vạch giữa ra ngoài theo độ lớn điểm, cùng một thang đối xứng cho mọi mã trong bản tin. Mã có điểm bằng 0 hiện `0.00` cạnh tên mã; hai cột vẫn có thể hiện cực trị lịch sử. Hai ô dưới bảng ghi riêng tổng điểm tăng/giảm hiện tại và cực trị trong phiên của từng tổng; cực trị tổng được tính từ từng bản tin rồi lưu trong `index_influence_total_extrema`, không phải tổng các cực trị riêng lẻ từng mã. Box 10 cập nhật theo `/api/ws/influence`, không chịu ảnh hưởng của ô Khung box 7 hoặc lịch sử tải sau đó. Chỉ các mã có trong bản tin mới nhất được vẽ.

Box 11 dưới box 10 vẽ hai thanh tỷ lệ số mã **tăng / giảm / đứng giá** từ bản tin VN30 chính thức và VN30 dự tính mới nhất qua `/api/ws/index`. Mỗi thanh ghi cả số mã và tỷ lệ theo tổng ba nhóm của chính bản tin đó; không mặc định tổng luôn là 30. Thiếu một trong ba số thì thanh tương ứng hiện “Chưa có số liệu”, không gán số thiếu thành 0. Khi tải lại, dùng bản tin mới nhất đã lưu của từng kênh. Đây là độ rộng thị trường DNSE cung cấp, không suy ra từ dữ liệu điểm đóng góp của box 7–10.

Box 12 là biểu đồ 1D dạng vùng tô như ảnh tham chiếu: **VN30 chính thức làm đường chuẩn ngang tại 0** từ 09:00 đến 14:45 UTC+7; vùng xanh khi phái sinh cao hơn VN30, vùng đỏ khi thấp hơn. Trục dọc bên phải là **basis = giá khớp cuối hợp đồng − VN30 chính thức** tính bằng điểm, không phải mức giá tuyệt đối. Chỉ ghi điểm khi đã có cả giá hợp đồng và chỉ số, bắt đầu từ 09:00. Bảng `basis_history` của ngày lưu bản cuối từng giây cho cả VN30 chính thức và dự tính; biểu đồ vẽ kênh chính thức, lấy bản cuối mỗi 5 giây qua `/api/basis-history?day=YYYY-MM-DD&bucket=5`, để trống nếu hai điểm cách nhau hơn 30 giây. Ô Ngày cho phép xem file backup của ngày cũ; ngày chưa thu dữ liệu không được nội suy. Bảng này chỉ lưu thời gian, tên kênh và basis đã tính, không lưu bản Quote hoặc Trade.

Để kiểm tra khi box 7–11 hoặc VN30 dự tính không có dữ liệu, chạy `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python orderflow-demo/diagnose_dnse_vn30.py --seconds 30 --encoding both` trong terminal có mạng. Script đọc `.env`, mở kết nối DNSE riêng, đăng ký ba kênh VN30 và in log ngay theo từng bước kết nối/xác thực/subscribe/ACK/bản tin đầu tiên, kèm trạng thái chờ định kỳ 3 giây. Cuối mỗi phép thử, nó so sánh số bản tin thô (`mi`/`emi`/`ii`), số callback SDK, số bản tin được ứng dụng chuẩn hóa và số bản ghi SQLite trước/sau. `stage_by_stream` cho biết `not_connected`, `no_raw_frame` (chưa thấy khung dữ liệu từ kênh trong thời gian thử), `sdk_dispatch_failed`, `app_normalization_failed` hoặc `normalized_ok`. `connected=false` nghĩa là phép thử chưa qua bước kết nối, nên không thể kết luận kênh DNSE có phát dữ liệu hay không. `subscriptions_sent` chỉ xác nhận SDK đã gửi lệnh đăng ký; `subscribed_acks` mới đếm phản hồi xác nhận. Script che API key/secret, chỉ in tên kênh, cấu trúc payload và số lượng, không in nội dung bản tin; không ghi vào SQLite. Dùng `--local-only` để chỉ kiểm tra file SQLite hiện tại mà không mở WSS.

```dotenv
DNSE_API_KEY=...
DNSE_API_SECRET=...
DNSE_WSS_URL=wss://ws-openapi.dnse.com.vn
DNSE_SYMBOL=41I1GB000
```

Với trang HTTP cục bộ, trình duyệt kết nối `ws://localhost:8097/api/ws`. Để dùng **WSS**, cung cấp chứng chỉ TLS được trình duyệt tin cậy bằng `--tls-cert /path/to/fullchain.pem --tls-key /path/to/privkey.pem` hoặc `ORDERFLOW_TLS_CERT` và `ORDERFLOW_TLS_KEY` trong `.env`, rồi mở `https://localhost:8097/orderflow-live.html`. Trang tự chọn `wss://` khi chạy qua HTTPS. WebSocket tự kết nối lại nếu bị ngắt.

Có thể đổi mã bằng `--symbol`. Mã mặc định theo bản Replay hiện tại, không tự suy ra hợp đồng gần nhất từ tên VN30F1M. Mặc định chỉ lắng nghe `127.0.0.1`; `--host 0.0.0.0` nếu cần truy cập từ máy khác trong mạng tin cậy. Chỉ có API dữ liệu thị trường, không gọi API đặt lệnh. `.env` không nằm trong thư mục phục vụ web.

### Lưu trữ và khôi phục

Mỗi ngày/mã có một file `data/live/<SYMBOL>/YYYY-MM-DD.sqlite`. SQLite dùng WAL + synchronous FULL; Quote lưu ảnh các mức giá ở dạng MessagePack tick/quantity, chỉ nén zlib khi nhỏ hơn bản chưa nén. Không lưu lặp các trường tên mã/board trên mỗi bản ghi. Trade có cột số nguyên và chỉ mục thời gian riêng để lọc lịch sử. Giữ thời gian nguồn đến mili giây, thời gian nhận và số thứ tự nhận trong file. Đây là lựa chọn cân bằng dung lượng và truy vấn trực tiếp; không khẳng định dung lượng nhỏ nhất trong mọi trường hợp.

Khi SDK nhận Quote hoặc Trade Extra, backend cập nhật ngay cache LIVE trong RAM cho DOM và Order Flow rồi xếp bản ghi vào hàng đợi SQLite riêng. WebSocket được đánh thức khi cache đổi, không đợi commit đĩa hoặc vòng kiểm tra 100 ms; trình duyệt giữ bản tin mới nhất nếu nhiều thông điệp đến trong cùng một khung hình. Writer gom tối đa khoảng 50 ms rồi commit SQLite. Replay và API lệnh lớn đọc phần đã commit, nên có thể chậm hơn LIVE tối đa một batch khi ghi đĩa hoạt động bình thường. Độ trễ còn phụ thuộc thời điểm DNSE gửi bản tin, thời gian tạo snapshot và thời gian vẽ trên máy xem.
Đánh giá xu hướng 30s/60s/180s làm mới nhiều nhất mỗi 0,5s để không giữ khóa truy cập kho dữ liệu trên mỗi Quote; khi đang đợi lượt tính tiếp theo, kết quả cũ giữ nguyên mốc `as_of` để không bị hiểu là đã tính dữ liệu mới. Giá, sổ lệnh và tốc độ khớp vẫn dùng bản tin mới nhất đã ghi.

Mẫu 10.000 Quote đầu file ngày 16/10/2026: JSONL 7.823.188 bytes, SQLite sau checkpoint 1.392.640 bytes (17,8%). Đây là phép đo trên mẫu, không phải cam kết dung lượng cho mọi phiên. Luồng LIVE dùng cache cập nhật gia tăng, chỉ dựng lại cửa sổ đánh giá gần nhất khi có dữ liệu mới.

Ghi theo batch tối đa 2.000 bản ghi hoặc khoảng 50 ms. Tắt bằng Ctrl+C sẽ ngắt nguồn và flush hàng đợi; khi mất điện/kill cưỡng bức có thể mất phần chưa commit/hàng đợi dù chúng từng hiện trên LIVE. Các transaction đã commit được SQLite phục hồi khi mở lại. Hàng đợi tối đa 100.000 sự kiện; đầy hàng đợi hoặc lỗi ghi đĩa sẽ báo lỗi và dừng thu thay vì âm thầm bỏ dữ liệu.

Khởi động lại cùng ngày sẽ mở file cũ, giữ lịch sử và tiếp tục thêm dữ liệu. Hai kênh VN30 chính thức/dự tính được ghi thêm vào bảng `market_index_history` cho từng bản tin được chấp nhận; `market_index_latest` vẫn giữ bản mới nhất cho giao diện. Chỉ giữ file LIVE của ngày hiện tại (UTC+7) cho mã đang chạy. Khi khởi động hoặc qua nửa đêm, backend đóng và flush phiên cũ, tạo backup `data/live-backup/<SYMBOL>/YYYY-MM-DD.sqlite` gồm `metadata`, `gaps`, `index_influence`, `index_influence_extrema`, `index_influence_total_extrema`, `market_index_latest`, `market_index_history` và `basis_history`, rồi mới xóa file LIVE cũ cùng WAL/SHM/journal. Backup không có bảng `events`, nên không chứa bản ghi Quote hoặc Trade. Nếu backup lỗi, file LIVE cũ được giữ lại. Với `--data-dir` tùy chỉnh, thư mục backup nằm cạnh thư mục đó và có hậu tố `-backup`. Dữ liệu VN30 của những ngày đã bị xóa trước khi áp dụng thay đổi này không thể khôi phục từ hệ LIVE. Không xóa thư mục raw Quote/Tape của bản Replay cũ. Tạm dừng, tua hoặc phân trang box lệnh lớn chỉ dừng màn hình, nguồn thu vẫn chạy. Bấm **LIVE** để theo dữ liệu mới nhất. Khi chuyển trang lệnh lớn, giao diện cố định mốc thời gian để giao dịch mới không đẩy lệch các dòng đang xem.

Xem dữ liệu đã thu của ngày hiện tại mà không kết nối DNSE:

```bash
.venv/bin/python orderflow-demo/live_server.py --offline --symbol 41I1GB000 --port 8097
```

### API

| Endpoint | Chức năng |
|---|---|
| `GET /api/meta` | Mã/ngày, giới hạn thời gian, số bản ghi và trạng thái nguồn |
| `GET /api/status` | Metadata cùng trạng thái kết nối, số bản ghi lỗi và hàng đợi |
| `WS /api/ws?tf=60&window=600` | WebSocket `state` khi cache LIVE đổi và `persisted` khi Trade Tape được ghi đĩa; heartbeat `state` mỗi 2 giây; trình duyệt tự kết nối lại |
| `GET /api/state?tf=60&window=600` | Trạng thái mới nhất; thêm `t` để xem lịch sử, tính bằng giây từ `base_ms` |
| `GET /api/large-trades?large_min=50&side=ALL&limit=100` | Lịch sử giao dịch lớn, mới nhất trước; tổng count/BUY/SELL trên tập đã lọc |
| `GET /api/basis-history?day=YYYY-MM-DD&bucket=5` | Lịch sử basis theo ngày của VN30 chính thức/dự tính; `bucket` nhận 1, 5, 15 hoặc 60 giây; ngày cũ đọc từ backup |
| `GET /api/ohlcv-1m` | Nến 1 phút đã nhận trong ngày của hợp đồng hiện tại, gồm nến đang hình thành và đã đóng |

API lệnh lớn hỗ trợ `side=ALL/BUY/SELL`, `large_min` là ngưỡng **lớn hơn**, `limit=1..500`, `from` và `t` là mốc giây tương đối từ `base_ms`. Không truyền `t` thì dùng dữ liệu mới nhất. Truyền `before` bằng giá trị `next` từ trang trước để phân trang theo cặp `(timestamp, thứ tự nhận)`, xử lý cả giao dịch trùng timestamp. Để cố định toàn bộ tập kết quả khi phân trang, giữ nguyên `t=as_of` từ trang đầu. Giá trong mảng `trades` là số tick 0,1 điểm, định dạng `[second, price_ticks, qty, side]`.

API state/WebSocket không nhét toàn bộ lệnh lớn vào mỗi bản cập nhật; box này tải API riêng. Tua lại chỉ tính Trade/Quote đến mốc chọn; bản ghi đến trễ làm cache lịch sử được dựng lại để không bỏ sót, còn cache LIVE cập nhật gia tăng theo timestamp nguồn.

### Giới hạn dữ liệu

Chỉ có dữ liệu từ lúc chương trình thu được; không tự tải bù đoạn trước khi chạy hoặc mất mạng. Metadata luôn đánh dấu `partial=true`; khoảng gián đoạn được lưu trong bảng `gaps`. Trong vùng 180s giao với gián đoạn hoặc khi nguồn chưa kết nối, đánh giá dòng lệnh bị tắt. Quote quá 30s được đánh dấu không khả dụng, ngoại trừ chính sách giữ dữ liệu giờ nghỉ 11:30–13:00 có sẵn. Không tự tạo giao dịch.

Không có trade ID từ nguồn: chỉ bỏ bản Trade Extra trùng hoàn toàn về timestamp/giá/khối lượng/chiều/cumulative volume khi cumulative volume tồn tại. Khi thiếu cumulative volume giữ mọi bản ghi, không tuyên bố chống trùng tuyệt đối. Hai nguồn trùng timestamp được hiển thị Quote trước rồi thứ tự nhận; không khẳng định đây là thứ tự sự kiện tại sàn. SDK 2.4.1 được bọc để chấp nhận cả `quantity` và `qtty` ở mức giá Quote.

Kiểm tra:

```bash
.venv/bin/python -m unittest discover -s orderflow-demo -p 'test_*.py'
node orderflow-demo/test_live_ui.cjs
node orderflow-demo/test_static_replay.cjs
```
