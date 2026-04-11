# 🚀 Dynamic POS EMI & Reward Negotiation System

## 📌 Overview
This project implements a real-time Point-of-Sale (POS) transaction interception system that allows users to:
- Convert transactions into EMI
- Apply reward points
- Use cashback offers

before finalizing payments.

---

## 🏗️ Architecture
- Event-Driven using Kafka
- CQRS Pattern
- Saga-like flow

---

## 🛠️ Tech Stack
- Java 17
- Spring Boot
- Kafka
- MongoDB
- PostgreSQL

---

## 📡 API Endpoints
POST /api/v1/transactions  
GET /api/v1/transactions/{id}/offers  
POST /api/v1/transactions/{id}/select-offer  
GET /api/v1/transactions/{id}

---

## 🚀 How to Run
1. Start Kafka (docker-compose)
2. Start MongoDB
3. Start PostgreSQL
4. Run Spring Boot

---

## 🔥 Features
- Real-time processing
- Async Kafka flow
- Dynamic offer generation
- Polyglot persistence

---

## 🏆 Hackathon Project
