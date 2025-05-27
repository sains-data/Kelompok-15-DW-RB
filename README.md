# Implementasi Medallion Architecture dengan Apache Spark untuk Pengelolaan dan Pengelompokan Data Transaksi E-Commerce

Selamat datang di repositori proyek **Data Warehouse untuk Optimalisasi Kinerja Kapal Berbasis Big Data**! 🚢\
Proyek ini menampilkan solusi lengkap dalam merancang arsitektur data warehouse yang bertujuan untuk mendukung pengambilan keputusan strategis dalam sektor operasional kapal. Dikembangkan sebagai bagian dari tugas besar akademik, repositori ini mengintegrasikan praktik terbaik dalam pemodelan data multidimensi, perancangan pipeline ETL, serta analisis berbasis data yang relevan dengan kebutuhan industri pelayaran modern.

---
## 👥 Anggota Tim
1. **Ibnu Farhan Alghifari** - 121450121
2. **Raid Muhammad Naufal** - 122450027
3. **Berliana Enda Putri** - 122450065
4. **Danang Hilal Kurniawan** - 122450085
5. **Akmal Faiz Abdillah** - 122450114
6. **Fayyaza Aqila Syafitri Achjar** - 122450131

---
## 🎯 Tujuan Proyek
* Mengidentifikasi kebutuhan data dan sumber informasi dari sistem operasional kapal seperti sensor teknis, navigasi, cuaca, posisi, dan logistik.
* Menerapkan proses derivasi untuk membentuk entitas fakta dan dimensi berdasarkan data mentah yang telah dikonsolidasi.
* Merancang arsitektur data warehouse berbasis skema bintang untuk mendukung analisis multidimensi terhadap performa kapal.
* Menyusun dokumentasi lengkap yang mencakup desain konseptual, logikal, dan fisik sebagai portofolio akademik dan profesional.

---
## 📊 Arsitektur Data Warehouse

Arsitektur data warehouse dalam proyek ini menggunakan pendekatan multidimensional dan diselaraskan dengan prinsip Medallion Architecture:
1. **Bronze Layer** – Data mentah dari sistem sumber (sensor, AIS, cuaca, dll).
2. **Silver Layer** – Data hasil transformasi, pembersihan, dan standarisasi.
3. **Gold Layer** – Data siap analisis dalam bentuk skema bintang (Star Schema).

---
## 📁 Struktur Direktori
```
KELOMPOK-15-DW-RB/
├── README.md
├── /images                 # Gambar dan visualisasi
├── /reports                # Laporan dan dokumentasi
│   ├── /1_Spesifikasi Kebutuhan        
│   ├── /2_Skema Data Konseptual
│   ├── /3_Skema Data Logikal & Fisikal   
└── /scripts                # Kode untuk ETL dan transformasi data
    ├── bronze/
    ├── silver/
    └── gold/
```

---
## 🛠️ Alat & Teknologi yang digunakan
* **SQL** – Untuk pengelolaan database dan eksekusi query.
* **DrawIO** – Untuk perancangan diagram skema bintang.
* **GitHub** – Manajemen versi dan dokumentasi kolaboratif.

---
## 👤 Tentang Kami
Proyek ini dikembangkan oleh mahasiswa Program Studi Sains Data, Fakultas Sains, Institut Teknologi Sumatera (ITERA), sebagai bagian dari Tugas Besar Mata Kuliah Pergudangan Data tahun 2025.
