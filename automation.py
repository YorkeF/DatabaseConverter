import getInsertStatements as gi
import NewCCTVCombineTables as nc
import StillwaterNewCCTVConversion as sc
import os, time, subprocess
import mysql.connector
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# import main
global mysql_host, mysql_user, mysql_password, mysql_database_name, current_directory, mysql_db_extension


# from main import mysql_host, mysql_user, mysql_password, mysql_database_name, current_directory
# Replace these values with your MySQL server details if not using gui in main
# mysql_host = "localhost"
# mysql_user = "root"
# mysql_password = "admin"
# mysql_database_name = "infratie_scripts"
# current_directory = os.getcwd()


# Function to delete a database
def delete_database(database_name):
    print("\n\nDeleting Database: " + "\n----------")
    try:
        connection = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
        )
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS `{database_name}`")
        print(f"\tSuccessfully deleted database: {database_name}")
    except Exception as e:
        print(f"\tFailed to delete database: {database_name}")
        print("\t" + e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def delete_files_with_meta():
    # current_directory = os.getcwd() Swapping out with main gui
    print("\n\nDeleting meta files:\n----------")
    meta_files = [file for file in os.listdir(current_directory) if "_Meta" in file]
    print(meta_files)
    print("\tFiles with '_meta' in the name:", meta_files)

    for meta_file in meta_files:
        file_path = os.path.join(current_directory, meta_file)
        print(file_path)
        try:
            os.remove(file_path)
            print(f"\tDeleted file: {meta_file}")
        except Exception as e:
            print(f"\tFailed to delete file: {meta_file}")
            print("\t" + e)


def rename_db3_files():
    delete_files_with_meta()
    print("\n\nRenaming db3 files:\n----------")

    # current_directory = os.getcwd()  Swapped in login declaration.
    db3_files = [file for file in os.listdir(current_directory) if file.endswith(".db3")]
    print(db3_files)
    for db3_file in db3_files:
        if " " in db3_file:
            new_file_name = (
                db3_file
                .replace(" ", "_")
                .replace(".", "_")
                .replace("-", "_")
                .replace("'", "_")
                .replace("(", "_")
                .replace(")", "_")
                .replace("&", "_")
                .replace("#", "_")
                .replace("_db3", ".db3")
            )
            new_file_name = new_file_name[-54:]

            # new_file_name = db3_file.replace(" ", "_").replace(".", "_").replace("_db3", ".db3")
            old_path = os.path.join(current_directory, db3_file)
            new_path = os.path.join(current_directory, new_file_name)

            try:
                os.rename(old_path, new_path)
                print(f"\tRenamed: {db3_file} -> {new_file_name}")
            except Exception as e:
                print(f"\tFailed to rename: {db3_file}")
                print("\t" + e)


def automation():
    # Call the function to delete files with '_meta' in the name
    # global mysql_host, mysql_user, mysql_password, mysql_db_extension, mysql_database_name, current_directory

    # set_mysql_host(host)
    # set_mysql_user(user)
    # set_mysql_password(password)
    # set_mysql_db_extension(db_extension)
    # set_mysql_database_name(database_name)
    # set_current_directory(directory)
    print(mysql_host + ", " + mysql_user + ", " + mysql_password + ", " + mysql_db_extension + ", " + mysql_database_name + ", " + current_directory)

    #delete_files_with_meta()
    # Call the function to rename .db3 files
    # delete_files_with_meta()
    rename_db3_files()

    print("\n\nMain Automation:\n----------")

    # current_directory = os.getcwd() Moved to top
    db3_files = [file for file in os.listdir(current_directory) if file.endswith(".db3")]
    print(db3_files)
    for db3_file in db3_files:

        new_file_name = (
            db3_file
            .replace(" ", "_")
            .replace(".", "_")
            .replace("-", "_")
            .replace("'", "_")
            .replace("(", "_")
            .replace(")", "_")
            .replace("&", "_")
            .replace("#", "_")
            .replace("_db3", ".db3")
        ).split(".")

        new_file_name[0] = new_file_name[0][-54:]

        # new_file_name = db3_file.replace(" ", "_").replace(".", "_").replace("_db3", ".db3").split(".")
        # print(new_file_name)
        try:
            gi.convert_to_csv(current_directory, new_file_name[0], new_file_name[1])
            gi.import_to_sql(new_file_name[0], current_directory)
            sc.main(new_file_name[0])
            nc.transfer_to_database(mysql_database_name, "converted_" + new_file_name[0])
            print("Success: " + new_file_name[0])
        except Exception as e:
            print("Failed: " + new_file_name[0])
            print(e)
        finally:
            # Delete the original and converted database files after the process
            try:
                # Delete the databases from MySQL
                delete_database(new_file_name[0])
                delete_database("converted_" + new_file_name[0])
            except Exception as e:
                # print("Failed to delete database files: " + db_file_path + " and " + converted_db_file_path)
                print(e)


# ---------------------------------------------------------------------------------------------



# mysql_host = "localhost"
# mysql_user = "root"
# mysql_password = "admin"
# mysql_database_name = "infratie_scripts"
# current_directory = os.getcwd()

def on_run():
    global mysql_host, mysql_user, mysql_password, mysql_db_extension, mysql_database_name, current_directory

    mysql_host = host_entry.get()
    mysql_user = user_entry.get()
    mysql_password = password_entry.get()
    mysql_db_extension = db_selector.get()
    mysql_database_name = db_name_entry.get()  # Get the actual database name from the Entry widget
    file_path = file_path_text.get("1.0", tk.END).strip()
    current_directory = file_path

    if not mysql_host or not mysql_user or not mysql_password or not mysql_db_extension or not file_path:
        messagebox.showerror("Error", "All fields must be filled!")
        return

    try:
        inputfileNameExt = file_path.split('.')
        matching_files = [file for file in os.listdir(file_path) if file.endswith(f".{mysql_db_extension}")]
        if not matching_files:
            messagebox.showinfo("Info", f"No {mysql_db_extension} files found in the selected folder.")
        else:
            automation()

        # messagebox.showinfo("Info", f"Success: {inputfileNameExt[0]}")
    except Exception as e:
        messagebox.showerror("Error", f"Failed: {inputfileNameExt[0]}\n{str(e)}")


def choose_directory_and_update_count(text_widget, db_type):
    directory = filedialog.askdirectory()
    if not directory:
        return

    matching_files = [file for file in os.listdir(directory) if file.endswith(f".{db_type}")]

    if not matching_files:
        messagebox.showinfo("Info", f"No {db_type} files found in the selected directory.")
    else:
        console_output = "\n".join(matching_files)
        print("Matching Files:")
        print(console_output)

    count = len(matching_files)
    update_text_widget(file_path_text, f"{directory}")
    update_text_widget(file_count_text, "Number of ." + db_type + " files: " + str(count))


root = tk.Tk()
root.title("MySQL Configuration and File Conversion")

host_label = tk.Label(root, text="MySQL Host:")
host_label.pack(pady=10)
host_entry = tk.Entry(root, width=30)
host_entry.pack(pady=5)

user_label = tk.Label(root, text="MySQL User:")
user_label.pack(pady=10)
user_entry = tk.Entry(root, width=30)
user_entry.pack(pady=5)

password_label = tk.Label(root, text="MySQL Password:")
password_label.pack(pady=10)
password_entry = tk.Entry(root, width=30, show="*")
password_entry.pack(pady=5)

db_name_label = tk.Label(root, text="MySQL Database Name:")
db_name_label.pack(pady=10)
db_name_entry = tk.Entry(root, width=30)
db_name_entry.pack(pady=5)

db_label = tk.Label(root, text="Database Type:")
db_label.pack(pady=10)
db_selector = ttk.Combobox(root, values=["db3", "mdb", "sdf"], state="readonly")
db_selector.pack(pady=5)

file_path_label = tk.Label(root, text="Select Folder containing .db3 Files:")
file_path_label.pack(pady=10)
file_path_frame = tk.Frame(root)
file_path_frame.pack(pady=5, fill=tk.X, expand=True)
choose_file_button = tk.Button(file_path_frame, text="Choose Folder",
                               command=lambda: choose_directory_and_update_count(file_path_text, db_selector.get()))
choose_file_button.pack(side=tk.LEFT, padx=5)
file_path_text = tk.Text(file_path_frame, height=1, width=40, wrap=tk.NONE, state=tk.DISABLED)
file_path_text.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 5))

file_count_text = tk.Text(file_path_frame, height=1, width=40, wrap=tk.NONE, state=tk.DISABLED)
file_count_text.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 5))

run_button = tk.Button(root, text="Run Script", command=on_run)
run_button.pack(pady=20)


def update_text_widget(text_widget, content):
    text_widget.config(state=tk.NORMAL)
    text_widget.delete("1.0", tk.END)
    text_widget.insert(tk.END, content)
    text_widget.config(state=tk.DISABLED)


root.mainloop()

# if __name__ == '__main__':
