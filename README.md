# MFT Analyzer

**MFT Analyzer** is a forensic tool for extracting and analyzing the **NTFS Master File Table ($MFT)**.  
It provides an easy-to-use graphical interface for generating `$MFT` data from a disk and exploring it using **CSV-based analysis**.

---

## 🚀 Why MFT Analyzer?

Unlike other tools such as **MFTExplorer**, which primarily work directly on `.bin` files,  
**MFT Analyzer** processes and works with `.CSV` exports. This makes it:

- ✅ **Faster and more stable** for large datasets (millions of records)  
- ✅ **Search-friendly** (direct queries on CSV columns)  
- ✅ **Cross-compatible** (CSV is widely supported by data tools)  
- ✅ **Lightweight** — no heavy database required  
- ✅ **Direct filters in the UI** for quick forensic triage  

---

## 🔍 Features

### Main Features
- Generate `$MFT` files from NTFS drives  
- Convert `$MFT.bin` → `.CSV` for analysis  
- Load and explore CSV files with an interactive GUI  

### Built-in Filter Options
From the UI, investigators can directly filter:
- **Quick Search**: filename, path, or content  
- **File Filters**: filename patterns, extensions, path contains  
- **Size Filters**: min / max file size (bytes, KB, MB, GB)  
- **Date Filters**: by created/modified/accessed timestamps  
- **Attribute Filters**:  
  - Directories only  
  - Files with Alternate Data Streams  
  - Deleted files  
  - Cooled files  
  - Timeline anomalies (`$SI < $FN`)  

⚡ These filters make it faster to pinpoint artifacts without external tools.

---

## 🛠️ Internal Tools Used

MFT Analyzer leverages powerful forensic utilities under the hood:

- **[RawCopy.exe](https://github.com/jschicht/RawCopy)**  
  → For raw NTFS file extraction (`$MFT` from disk)  

- **[MFTECmd.exe](https://ericzimmerman.github.io/#!index.md)**  
  → For converting raw `$MFT` (`.bin`) into human-readable `.CSV`  

Both tools are bundled with the application inside the `tools/` directory.

---

## 📦 Installation & Usage

- Download `MFTAnalyzer.exe` and Open with `Run as Administrator`

---

## 🛠️ Troubleshooting

If you face issues running **MFT Analyzer.exe**, check the following:

### 1. Run as Administrator
RawCopy requires direct disk access.  
Always launch `MFTAnalyzer.exe` with **administrator privileges**:  
- Right-click → **Run as Administrator**  
- Or set permanently via:  
  - Right-click `MFTAnalyzer.exe` → **Properties** → **Compatibility** →  
    Check **Run this program as administrator**
    

### 2. Verify .NET Runtime

#### Check .NET Runtime (Core)
Run in **PowerShell**:
```powershell
dotnet --list-runtimes
```
Expected output should include:

`Microsoft.NETCore.App 9.0.x [C:\Program Files\dotnet\shared\Microsoft.NETCore.App]`

### 3. Check .NET Framework (legacy)

Run in CMD:
```cmd
reg query "HKLM\SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full" /v Release
```

`394802` or higher → ✅ .NET 4.6+ installed

`528040` or higher → ✅ .NET 4.8 installed

### Install if missing

[Install .NET Framework 4.8](https://dotnet.microsoft.com/en-us/download/dotnet-framework/net48)

[Install .NET Runtime 9.0 (x64)](https://dotnet.microsoft.com/en-us/download/dotnet/thank-you/runtime-9.0.9-windows-x64-installer?cid=getdotnetcore)

[Install VC++ Runtime 2015–2019 (x64)](https://aka.ms/vs/16/release/vc_redist.x64.exe)

---
## 📷 Screenshots

### Dashboard
![](/image/01.png)
![](/image/02.png)
![](/image/03.png)

## 📜 License & Disclaimer

MFT Analyzer is provided for educational and authorized forensic use only.

### You may use this tool on:

- ✅ Your own systems
- ✅ Systems where you have explicit written consent from the owner (e.g., client engagements)
- ✅ In a corporate/enterprise environment if you are part of the security/forensics team and authorized by your employer
- ✅ As part of official law enforcement investigations under legal authority (e.g., warrant, subpoena)

### You may not use this tool for:

- ❌ Unauthorized access to other people’s devices
- ❌ Any activity that violates local, state, or international laws

The authors/contributors are not responsible for any misuse of this tool.
By using MFT Analyzer, you agree to ensure you have the legal right and permission to analyze the target system.
  
