# README

## Report Generation Instructions

This guide provides detailed steps for generating reports using the provided Python scripts. Ensure you follow the instructions carefully to set up your environment and execute the scripts successfully.

---

### Local Setup

1. **Python Environment**:
   - Ensure you have **Python 3.13.0** installed.
   - If Python 3.13.0 is unavailable globally, set up a virtual environment:
     ```bash
     python -m venv venv
     source venv/bin/activate  # For macOS/Linux
     .\venv\Scripts\activate  # For Windows
     ```
   - Verify the Python version inside the virtual environment:
     ```bash
     python --version
     ```

2. **Install Dependencies**:
   - Install the required packages from the `requirements.txt` file:
     ```bash
     pip install -r requirements.txt
     ```

---

### Connecting to the Database

1. **OpenVPN Connection**:
   - Open **OpenVPN Connect**.
   - Enter your credentials:
     - **User**: Provide your username.
     - **Comma**: ,
     - **Okta Code**: Enter the OTP generated by Okta.
   - Eg : abc@7,okta-code

2. Ensure the VPN connection is active before proceeding to execute the scripts.

---

### Running the Scripts

#### **Notifier Report**

1. Update the following parameters in the `generate-notifier-report.py` script:
   - `shipper_id`: Example: `'smithfield-foods'`
   - `start_date`: Example: `'2024-11-07'`
   - `end_date`: Example: `'2024-11-07'`

2. Execute the script to generate the Notifier report:
   ```bash
   python generate-notifier-report.py
3. The final report will be saved in the `dist` folder.



#### **RTP Report**

### Step 1: Generate Holdover Report

1. Navigate to the `utils` folder:
   ```bash
   cd utils

2. Update the following parameters in the script:
    - `shipper_id_holdover`: Specify the shipper, e.g., `'smithfield-foods'`.
    - `start_date`: Example: `'2024-11-07'`
    - `end_date`: Example: `'2024-11-07'`
    - `gmail username` and `password`: Provide your gmail credentials.

3. Run the script to generate the holdover report:
    ```bash
    python get-holdover.py

4. After generating the report, exit the utils folder:
    ```bash
    cd ..

### Step 2: Generate RTP Report

1. Before executing the `generate-rtp-report.py`, ensure that the `shipper_id`, `start_date`, and `end_date` in this script match those used in the holdover report.

2. Execute the script to generate the RTP report:
    ```bash
    python generate-rtp-report.py

3. The final RTP report will be saved in the `dist` folder.

### Notes
1. Ensure that the date ranges for all scripts are consistent.
2. Always confirm VPN connectivity before running scripts that interact with the database.
3. Reports will be saved in the `dist` folder by default.