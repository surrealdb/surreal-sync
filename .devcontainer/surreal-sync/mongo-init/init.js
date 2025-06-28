// MongoDB initialization script for surreal-sync development
// This script creates a sample database and collection for testing

db = db.getSiblingDB('sample_db');

// Create a sample collection with test data
db.users.insertMany([
    {
        _id: ObjectId(),
        name: "John Doe",
        email: "john@example.com",
        age: 30,
        created_at: new Date(),
        tags: ["developer", "javascript"],
        profile: {
            bio: "Full stack developer",
            location: "San Francisco"
        }
    },
    {
        _id: ObjectId(),
        name: "Jane Smith",
        email: "jane@example.com",
        age: 25,
        created_at: new Date(),
        tags: ["designer", "ui/ux"],
        profile: {
            bio: "UI/UX Designer",
            location: "New York"
        }
    },
    {
        _id: ObjectId(),
        name: "Bob Johnson",
        email: "bob@example.com",
        age: 35,
        created_at: new Date(),
        tags: ["manager", "agile"],
        profile: {
            bio: "Project Manager",
            location: "Chicago"
        }
    }
]);

// Create an index on email field
db.users.createIndex({ "email": 1 }, { unique: true });

print("MongoDB sample data initialized successfully!");