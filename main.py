from flask import Flask, request, jsonify
import sqlite3

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('books.db')
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            author TEXT NOT NULL,
            published_year INTEGER,
            genre TEXT
        )
        ''')
    return conn

@app.route('/books', methods=['POST'])
def add_book():
    data = request.json
    if not all(key in data for key in ['title', 'author', 'published_year', 'genre']):
        return jsonify({"error": "Invalid book data", "message": "All fields are required"}), 400
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO books (title, author, published_year, genre) VALUES (?, ?, ?, ?)',
                       (data['title'], data['author'], data['published_year'], data['genre']))
        conn.commit()
        conn.close()
        return jsonify({"message": "Book added successfully"}), 201
    except Exception as e:
        return jsonify({"error": "Database error", "message": str(e)}), 500

@app.route('/books', methods=['GET'])
def get_books():
    conn = get_db_connection()
    books = conn.execute('SELECT * FROM books').fetchall()
    conn.close()
    return jsonify([dict(book) for book in books])

@app.route('/books/<int:id>', methods=['GET'])
def get_book(id):
    conn = get_db_connection()
    book = conn.execute('SELECT * FROM books WHERE id = ?', (id,)).fetchone()
    conn.close()
    if book is None:
        return jsonify({"error": "Book not found", "message": "No book exists with the provided ID"}), 404
    return jsonify(dict(book))

@app.route('/books/<int:id>', methods=['PUT'])
def update_book(id):
    data = request.json
    conn = get_db_connection()
    book = conn.execute('SELECT * FROM books WHERE id = ?', (id,)).fetchone()
    if book is None:
        conn.close()
        return jsonify({"error": "Book not found", "message": "No book exists with the provided ID"}), 404
    conn.execute('UPDATE books SET title = ?, author = ?, published_year = ?, genre = ? WHERE id = ?',
                 (data['title'], data['author'], data['published_year'], data['genre'], id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Book updated successfully"})

@app.route('/books/<int:id>', methods=['DELETE'])
def delete_book(id):
    conn = get_db_connection()
    book = conn.execute('SELECT * FROM books WHERE id = ?', (id,)).fetchone()
    if book is None:
        conn.close()
        return jsonify({"error": "Book not found", "message": "No book exists with the provided ID"}), 404
    conn.execute('DELETE FROM books WHERE id = ?', (id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Book deleted successfully"})

if __name__ == '__main__':
    app.run(debug=True)
