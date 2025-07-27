# QuizAPI
RESTful API for managing questions, quizzes, and user interactions in an educational or competitive system.

# 📘 SualAPI

**QuizAPI** is a RESTful web API built to manage questions, quizzes, and user answers within a flexible and scalable system. It supports multiple use cases such as public question banks, private bot-specific quizzes, and user point tracking.

---

## 🚀 Features

- ✅ Create, update, and delete questions
- 🔄 Support for public and private question sets
- 📊 User submissions tracking
- 🧠 Points, rankings, and stats
- ⏱️ Time-limited quizzes and competitions
- 🗂️ Redis-based storage (high performance)
- 🔐 Token-based authentication (optional)

---
## 🧪 Example Endpoints

GET    /questions/public POST   /questions/private POST   /answers/submit GET    /stats/user/{user_id}

---

## ⚙️ Installation

```bash
git clone https://github.com/yourusername/SualAPI.git
cd SualAPI
pip install -r requirements.txt
uvicorn api.main:app --reload


---

📌 Environment Variables

Create a .env file and configure the following:

REDIS_URL=redis://localhost:6379
SECRET_KEY=your_secret_here


---

📬 License

MIT License


---

🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.


---

💡 Author

Made with ❤️ by Rabie A.
