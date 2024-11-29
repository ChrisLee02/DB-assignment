CREATE_DVD_TABLE = """
CREATE TABLE DVDs (
    d_id INT AUTO_INCREMENT PRIMARY KEY,  
    title VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,           
    director VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,        
    stock INT DEFAULT 2,
    CONSTRAINT unique_dvd UNIQUE (title, director)
);
"""

CREATE_USER_TABLE = """
CREATE TABLE Users (
    u_id INT AUTO_INCREMENT PRIMARY KEY,  
    name VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,            
    age INT NOT NULL CHECK (age > 0),     
    borrow_count INT DEFAULT 0 CHECK (borrow_count >= 0), 
    CONSTRAINT unique_user UNIQUE (name, age)
);
"""

CREATE_BORROW_TABLE = """
CREATE TABLE BorrowRecords (
    record_id INT AUTO_INCREMENT PRIMARY KEY, 
    d_id INT NOT NULL,                        
    u_id INT NOT NULL,                        
    status ENUM('borrowed', 'returned') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'borrowed', 
    rating INT DEFAULT NULL CHECK (rating BETWEEN 1 AND 5), 
    FOREIGN KEY (d_id) REFERENCES DVDs(d_id) ON DELETE CASCADE,
    FOREIGN KEY (u_id) REFERENCES Users(u_id) ON DELETE CASCADE
);
"""


CREATE_TABLES = [CREATE_DVD_TABLE, CREATE_USER_TABLE, CREATE_BORROW_TABLE]

DROP_TABLES = [
    "DROP TABLE IF EXISTS BorrowRecords",
    "DROP TABLE IF EXISTS Users",
    "DROP TABLE IF EXISTS DVDs",
]
