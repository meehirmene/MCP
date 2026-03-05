// FlowForge E-Commerce Init for MongoDB
// Creates collections for event data and clickstream

db = db.getSiblingDB('ecommerce');

// Create collections with schemas
db.createCollection('events', {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["event_type", "timestamp"],
            properties: {
                event_type: { bsonType: "string" },
                timestamp: { bsonType: "date" },
                user_id: { bsonType: "int" },
                session_id: { bsonType: "string" },
                data: { bsonType: "object" }
            }
        }
    }
});

db.createCollection('clickstream');
db.createCollection('user_sessions');
db.createCollection('product_views');

// Insert sample events
db.events.insertMany([
    {
        event_type: "page_view",
        timestamp: new Date(),
        user_id: 1,
        session_id: "sess_001",
        data: { page: "/products/wireless-headphones", referrer: "google.com" }
    },
    {
        event_type: "add_to_cart",
        timestamp: new Date(),
        user_id: 1,
        session_id: "sess_001",
        data: { product_id: 1, quantity: 1, price: 79.99 }
    },
    {
        event_type: "checkout",
        timestamp: new Date(),
        user_id: 2,
        session_id: "sess_002",
        data: { order_id: 2, total: 129.99, items: 1 }
    },
    {
        event_type: "page_view",
        timestamp: new Date(),
        user_id: 3,
        session_id: "sess_003",
        data: { page: "/products/coffee-maker", referrer: "facebook.com" }
    },
    {
        event_type: "purchase",
        timestamp: new Date(),
        user_id: 1,
        session_id: "sess_001",
        data: { order_id: 1, total: 159.98, payment_method: "credit_card" }
    }
]);

print("MongoDB e-commerce collections initialized");
