# 🧩 Task 1 - Hadoop MapReduce

Với khối lượng thông tin được tạo ra ngày càng lớn và phức tạp, các phương pháp xử lý truyền thống trên một máy chủ đơn lẻ không còn đáp ứng được yêu cầu về tốc độ và hiệu suất khi làm việc với dữ liệu lớn. Vì vậy, một mô hình tính toán phân tán như Hadoop MapReduce trở thành giải pháp tối ưu, giúp xử lý dữ liệu một cách hiệu quả và có thể mở rộng theo nhu cầu thực tế.

Hadoop MapReduce là một Apache framework, được thiết kế để xử lý các tập dữ liệu có kích thước rất lớn bằng cách phân chia và xử lý đồng thời trên nhiều máy chủ trong một cụm máy tính (cluster).

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

## Kết quả thực thi

Kết quả (6 dòng đầu tiên) xét trên 728 ngày khách hàng mua sắm
| Ngày | Nhóm khách hàng | Số lượng |
|------------|---------------------|---------------------|
| 01/01/2014 | 4260, 3681, 1440, 2351, 1381, 1789, 1249<br>2943, 2226, 2974, 2237, 2610, 2542, 2709<br>3797, 4942, 3942, 2727, 1659, 1922, 3956 | 21 |
| 01/01/2015 | 1220, 3240, 2768, 1798, 2127, 1422, 1235<br>2851, 2641, 3026, 2211, 3751, 4104, 4537<br>4439, 4616 | 16 |
| 01/02/2014 | 4255, 3243, 3220, 3120, 2690, 2202, 4247<br>4887, 4746, 4548, 4527, 3758, 1878, 4140<br>1809, 3156, 1175, 1965, 3805, 2755, 3107<br>2475, 3663, 4316, 2717 | 25 |
| 01/02/2015 | 3682, 2384, 2383, 3064, 3086, 2051, 2271<br>1206, 3510, 3520, 4147, 3754, 4933 | 13 |
| 01/03/2014 | 4146, 2595, 1241, 1483, 1362, 2085, 3737<br>2514, 2602, 1864, 1764, 1335, 2279, 4664<br>4822, 4625, 2935, 3746, 3517, 1658, 1339 | 21 |
| 01/03/2015 | 4031, 3178, 2386, 2573, 2680, 2391, 2977<br>4529, 1107, 1546, 3316, 1136, 1684, 4766<br>3821, 1837, 2617, 4867, 4856, 3944 | 20 |

<br>

# 🧩 A-Priori Algorithm

Trong quá trình khai thác tập các cặp mục phổ biến (frequent pairs), ta có thể áp dụng các phương pháp lưu trữ dữ liệu đơn giản, sau đó chỉ cần đọc toàn bộ tập dữ liệu một lần duy nhất, với mỗi cặp mục được tạo ra, ta tăng giá trị đếm của nó lên 1.

Tuy nhiên, trong trường hợp dữ liệu quá lớn thì việc ghi nhớ và đếm toàn bộ các cặp mục trở nên bất khả thi. Để giải quyết vấn đề này, thuật toán A-Priori được đề xuất như một phương pháp cải tiến giúp giảm đáng kể số lượng cặp mục cần theo dõi. Trong A-Priori, một ứng viên (tập mục) được coi là phổ biến nếu $Support$ của nó lớn hơn hoặc bằng một ngưỡng tối thiểu ($min\_support$).

```math
Support(I) = \#\{\,basket \mid I \subseteq basket\,\}
```

```math
I_{\mathrm{frequent}}:\ Support(I) \ge \delta
```

Ngoài ra, thuật toán A-Priori cũng giúp sàng lọc bớt những cặp mục không có tiềm năng ngay từ sớm, nhờ tính chất: với mọi $X$ là tập con của $Y$, nếu $Support$ của tập $X$ không lớn hơn hoặc bằng $min\_support$ thì $Support$ tập $Y$ cũng không thể đạt ngưỡng.

```math
\forall X, Y:\ (X \subseteq Y) \land \left(support(X) < minSup\right)
\Rightarrow support(Y) < minSup
```

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

## Kết quả

Với $min\_support$ = 3, ta tìm được 6 cặp khách hàng thường xuyên
| Các cặp khách hàng thường xuyên | Support |
|-------------------------|---------|
| [2028, 2860] | 3 |
| [2432, 2893] | 3 |
| [2906, 4623] | 3 |
| [4074, 3593] | 3 |
| [4113, 3458] | 3 |
| [4121, 3465] | 3 |

Với các $min\_support$ khác nhau, ta có thống kê chung như sau:
| $min\_support$ | Số lượng các cặp khách hàng thường xuyên |
|-------------------------|---------|
| 2 | 1320 |
| 3 | 6 |
|4 | 0 |
