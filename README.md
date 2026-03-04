# 🧩 Hadoop MapReduce

Với khối lượng thông tin được tạo ra ngày càng lớn và phức tạp, các phương pháp xử lý truyền thống trên một máy chủ đơn lẻ không còn đáp ứng được yêu cầu về tốc độ và hiệu suất khi làm việc với dữ liệu lớn. Vì vậy, một mô hình tính toán phân tán như Hadoop MapReduce trở thành giải pháp tối ưu, giúp xử lý dữ liệu một cách hiệu quả và có thể mở rộng theo nhu cầu thực tế.

Hadoop MapReduce là một Apache framework, được thiết kế để xử lý các tập dữ liệu có kích thước rất lớn bằng cách phân chia và xử lý đồng thời trên nhiều máy chủ trong một cụm máy tính (cluster).

## Bộ dữ liệu

Bộ dữ liệu sử dụng trong giải thuật này là [Baskets](https://drive.google.com/file/d/1gFp_DnSGhTofebDKkUOFO42VoUWyDnUT/view?usp=sharing)

## Mã giả

```
Đầu vào:
	- Baskets: Danh sách các ngày chứa danh sách khách mua hàng.

Đầu ra:
	-  groupsOfCustomers: Nhóm khách hàng đi mua sắm trong cùng một ngày.

MyMapper:
    Bước 1: Đọc dữ liệu trong Baskets.
    Bước 2: Với mỗi dòng dữ liệu ghi nhận, xử lý chuỗi để trích xuất trường thông tin Date và Member_number. Sau đó, ghi nhận cặp key-value (Date, Member_number) vào tập kết quả. Hết bước này, chương trình thực hiện Group-by-Key (với key là Date) trên toàn bộ tập mục context ghi nhận được.

MyReducer:
    Bước 1: Nhận lần lượt các tập mục đã được gom nhóm theo Date và lọc bỏ các Member_number trùng lặp trong ngày đó.
    Bước 2: Ghi kết quả vào file trong midterm/output/task1.
```

<br>

# 🧩 A-Priori Algorithm

Trong quá trình khai thác tập các cặp mục phổ biến (frequent pairs), ta có thể áp dụng các phương pháp lưu trữ dữ liệu đơn giản, sau đó chỉ cần đọc toàn bộ tập dữ liệu một lần duy nhất, với mỗi cặp mục được tạo ra, ta tăng giá trị đếm của nó lên 1.

Tuy nhiên, trong trường hợp dữ liệu quá lớn thì việc ghi nhớ và đếm toàn bộ các cặp mục trở nên bất khả thi. Để giải quyết vấn đề này, thuật toán A-Priori được đề xuất như một phương pháp cải tiến giúp giảm đáng kể số lượng cặp mục cần theo dõi. Trong A-Priori, một ứng viên (tập mục) được coi là phổ biến nếu $Support$ của nó lớn hơn hoặc bằng một ngưỡng tối thiểu ($min\_support$).

Ngoài ra, thuật toán A-Priori cũng giúp sàng lọc bớt những cặp mục không có tiềm năng ngay từ sớm, nhờ tính chất: với mọi $X$ là tập con của $Y$, nếu $Support $ của tập $X$ không lớn hơn hoặc bằng $min\_support$ thì $Support $ tập $Y$ cũng không thể đạt ngưỡng.

Với bài toán này, các cặp khách hàng thường xuyên được xác định là **những cặp khách hàng thường xuất hiện cùng nhau trong cùng ngày**.

## Mã giả

```
Đầu vào:
	- Baskets: Danh sách các ngày chứa danh sách khách mua hàng (Kết quả từ Task 1).
	- s: Ngưỡng hỗ trợ tối thiểu.

Đầu ra:
	- frequentPairs: Danh sách các cặp mục phổ biến.

Pass 1:
    Mapper 1:
        Bước 1: Đọc dữ liệu theo từng ngày (Quét toàn bộ dữ liệu lần 1).
        Bước 2: Với mỗi dòng dữ liệu ghi nhận, tiền xử lý dữ liệu theo các bước:
            1. Chuẩn hóa dữ liệu: loại bỏ các ngoặc []
            2. Tách dữ liệu theo tab (\t) để tách ngày giao dịch và chuỗi danh sách khách hàng
            3. Sử dụng StringTokenizer để tách chuỗi danh sách khách hàng thành từng khách hàng riêng lẻ.
        Bước 3: Với từng khách hàng riêng lẻ, ghi nhận cặp key-value (Member_number, 1) vào tập kết quả. Hết bước này, chương trình thực hiện Group-by-Key (với key là Member_number) trên toàn bộ tập mục context ghi nhận được.

    Reducer 1:
        Bước 1: Nhận lần lượt các tập mục đã được gom nhóm theo Member_number.
        Bước 2: Cộng tổng số lần xuất hiện của từng tập mục. Nếu số lần xuất hiện ≥ s, ghi nhận vào file kết quả.

Pass 2:
    Mapper 2:
        Bước 1: Đọc dữ liệu theo từng ngày (Quét toàn bộ dữ liệu lần 2).
        Bước 2: Với mỗi dòng dữ liệu ghi nhận, tiền xử lý dữ liệu theo các bước:
            1. Chuẩn hóa dữ liệu: loại bỏ các ngoặc []
            2. Tách dữ liệu theo tab (\t) để tách ngày giao dịch và chuỗi danh sách khách hàng
            3. Sử dụng String splitting function để tách chuỗi danh sách khách hàng thành từng khách hàng riêng lẻ.
        Bước 3: Gom các khách hàng trong danh sách các khách hàng riêng lẻ thành từng cặp phân biệt để tạo thành một key mới, ghi nhận cặp key-value ([Member_number 1, Member_number 2], 1) vào tập kết quả. Hết bước này, chương trình thực hiện Group-by-Key (với key là [Member_number 1, Member_number 2]) trên toàn bộ tập mục context ghi nhận được.

    Reducer 2:
        Bước 1: Nhận lần lượt các tập mục đã được gom nhóm theo [Member_number 1, Member_number 2].
        Bước 2: Với mỗi tập mục ghi nhận, xử lý tập mục theo các bước:
            1. Tách dữ liệu theo tab (\t) để tách ra được key của tập mục là cặp khách hàng.
            2. Sử dụng String splitting function để tách cặp khách hàng thành từng khách hàng riêng lẻ.
        Bước 3: Kiểm tra từng khách hàng riêng lẻ trong key tương ứng có nằm trong Frequent-1-customer không.
            1. Nếu có, cộng tổng số lần xuất hiện của từng tập mục. Nếu số lần xuất hiện ≥ s, ghi nhận vào file kết quả.
            2. Nếu một trong hai khách hàng không thuộc Frequent 1-customer thì bỏ qua tập mục này.
```
