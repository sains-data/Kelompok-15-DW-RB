# **Spesifikasi Kebutuhan Perancangan Arsitektur Data Warehouse untuk Optimalisasi Kinerja Kapal Berbasis Big Data**

**Tujuan 🎯**: Menganalisis kebutuhan bisnis dan teknis untuk merancang Data Warehouse (DW) di industri kapal dan maritim.

---
## **📃 Daftar Isi**
1. [Profil Industri & Masalah Bisnis](#-profil-industri--masalah-bisnis)
2. [Daftar Departemen & Tujuan Bisnis](#-daftar-departemen--tujuan-bisnis)
3. [Fakta & Dimensi](#-fakta--dimensi)
4. [Sumber Data & Meta Data](#%EF%B8%8F-sumber-data--meta-data)
5. [Referensi](#-referensi)

--- 
## **🚢🔍 Profil Industri & Masalah Bisnis**
Transportasi maritim merupakan sektor bisnis global yang sangat kompleks dan menjadi tulang punggung utama perdagangan internasional. Dalam operasionalnya, perusahaan pelayaran menghadapi tantangan dalam mengamati performa kapal berdasarkan rute dan waktu, menganalisis pengaruh cuaca terhadap konsumsi bahan bakar, serta mengevaluasi status navigasi dan estimasi waktu kedatangan secara akurat. Selain itu, deteksi dini terhadap potensi masalah teknis melalui sensor mesin masih belum optimal karena data yang tersebar dan tidak terintegrasi. Profil bisnis ini menunjukkan bahwa dibutuhkan sistem data warehouse yang mampu mengintegrasikan berbagai sumber data secara real-time agar pengambilan keputusan dapat dilakukan secara cepat, akurat, dan berbasis data, guna meningkatkan efisiensi, keselamatan, dan daya saing industri pelayaran.

---
## **🏢🎯 Daftar Departemen & Tujuan Bisnis**
|Departemen|Tujuan Bisnis|Tujuan Penggunaan Big Data|
|---|---|---|
|Operasional| Mengoptimalkan konsumsi bahan bakar dan efisiensi operasional |  Menentukan kecepatan optimal kapal berdasarkan kondisi mesin dan cuaca|
|Perawatan dan Pemeliharaan|Mengurangi biaya pemeliharaan kapal dengan perawatan yang tepat waktu|Menentukan waktu dan jenis perawatan berdasarkan data konsumsi bahan bakar dan kondisi kapal
|Perencanaan Rute|Menyusun rute kapal yang efisien dan aman|Memanfaatkan data cuaca, estimasi waktu kedatangan, dan kondisi rute untuk merencanakan rute terbaik|
|Chartering|Meningkatkan transparansi dan daya saing dalam alokasi kapal|Mengintegrasikan data AIS, posisi kapal, ETA, dan data pasar untuk memberikan informasi lebih akurat dan efisien|
|Keamanan dan Keandalan|Memastikan kapal beroperasi dengan aman dan terhindar dari ancaman|Memantau kapal secara real-time dan mengantisipasi potensi ancaman atau gangguan keamanan|
|Manajemen Flotasi|Mengoptimalkan alokasi armada kapal untuk meningkatkan profitabilitas|Menggunakan data performa kapal dan data pasar untuk mengalokasikan armada secara efisien|
|Pengelolaan Terminal dan Pelabuhan|Mengoptimalkan alokasi dan penanganan kargo di pelabuhan|Menggunakan data ETA dan ketersediaan fasilitas pelabuhan untuk manajemen pelabuhan yang lebih efisien|
|Vetting dan Kualitas Kapal|Memastikan kapal memenuhi standar keselamatan dan kualitas yang dibutuhkan|Menganalisis data kapal dari berbagai sumber untuk memilih kapal yang paling sesuai dengan risiko terendah|

---
## **📊 Fakta & Dimensi**

Arsitektur data warehouse pada sistem ini menggunakan pendekatan multidimensional untuk mendukung analisis operasional kapal. Struktur ini terdiri dari satu tabel fakta utama, *`Fact_Ship_Data`*, yang mencatat aktivitas dan performa kapal, serta beberapa tabel dimensi yang menyediakan konteks analitik seperti kondisi cuaca, sensor kapal, waktu, rute, posisi kapal, navigasi, dan kapal itu sendiri. Tabel fakta dan dimensi ini saling terhubung melalui kunci asing, memungkinkan analisis yang lebih mendalam dan komprehensif terhadap data kapal.

---
## **🗂️ Sumber Data & Meta Data**
|Sumber Data|Deskripsi|Contoh Data|
|---|---|---|
|Sensor Kapal|Data yang berkaitan dengan performa, status, dan operasional kapal yang didapat dari berbagai sensor|Suhu mesin, tekanan, aliran bahan bakar, getaran mesin|
|Sistem AIS (Automatic Identification System)|Sistem untuk melacak posisi kapal secara real-time, memberikan informasi terkait posisi dan status kapal|Posisi kapal (latitude dan longitude), kecepatan, arah pelayaran, tujuan pelabuhan|
|Cuaca|Data cuaca yang mempengaruhi operasional kapal, termasuk kondisi atmosfer dan laut|Kecepatan angin, suhu udara, curah hujan, tinggi gelombang laut|
|Sistem Logistik dan Perencanaan Rute|Data yang berkaitan dengan perencanaan rute kapal|Rute pelayaran, pelabuhan asal dan tujuan, alokasi kapal|

---
## **📚 Referensi**
[1] United Nations Conference on Trade and Development (UNCTAD), Review of Maritime Transport 2021. [Online]. Available: https://unctad.org/publication/review-maritime-transport-2021 \
[2] Marine Digital, "Big data in maritime: How a shipping company can effectively use data". [Online]. Available: https://marinedigital.com/article_bigdata_in_maritime
