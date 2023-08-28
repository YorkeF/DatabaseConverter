# Setup and Usage

### 1. **Clone the Repository:**  
Clone the [DatabaseConverter repository](https://github.com/YorkeF/DatabaseConverter) from GitHub or unzip the provided file.

### 2. **Install Required Packages:**  
Install the required Python packages. The main packages used are `mysql-connector-python`, `tkinter`, and `pandas`.
Either run `pip install -r requirements.txt` or `pip install mysql-connector-python pandas`

### 3. **Configure MySQL Connection (Optional):**  
Open the `config.py` file and update the MySQL connection details:
   ```python
mysql_host = "localhost"
mysql_user = "root"
mysql_password = "admin"
mysql_database_name = "infratie_scripts"
current_directory = ""
   ```
If working properly, this should be overwritten by the inputs typed into the GUI, and is entirely optional.

### 4. **Run the Application:**
Run the `automation.py` script by typing `python automation.py` into the terminal. This script provides a GUI for configuring the MySQL connection and selecting the folder containing the database files to be converted.

### 5. **Configure and Run:**
- Enter the MySQL connection details in the GUI. 
- Select the database type (db3, mdb, sdf) from the dropdown menu.
- Select the folder containing the database files.
- Click the "Run Script" button. This will bring the data from the `db3` file into the specified `mysql` database.

### 6. **Note:**
As of now the project is only able to convert `db3` files. Because of this, features involving `mdb` or `sdf` files may not behave as intended.