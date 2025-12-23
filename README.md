
# Crypto Data Modeling Project

## Welcome 👋

Welcome to this project.

This repository is a **data modeling project** built to simulate how data analysts and analytics engineers work with **real-world, messy data** and transform it into **analytics ready data** that can be trusted for decision-making.

If you are a **technical user**, you’ll be able to navigate the project structure easily.  
If you are **non-technical**, don’t worry this README will walk you through what the project is, why it exists, and how to understand it at a high level.

---

## Project Context

This project is set in a **crypto / Web3 environment**, inspired by:
- P2P crypto transactions  
- On-chain activity  
- Data licensing and digital asset usage  

In real companies, raw data is often:
- inconsistent
- incomplete
- duplicated
- poorly structured for analysis

Because of this, analysts **cannot simply query raw tables** and start drawing insights.  
The data first needs to be **modeled**.

That is what this project demonstrates.

---

## What Is Data Modeling?

Data modeling is the process of:
- taking raw, operational data
- restructuring it
- standardizing it
- and transforming it into **clean, reliable, analytics-ready tables**

These final tables are what power:
- dashboards
- reports
- KPIs
- business decisions

This project focuses **specifically on that transformation process**.

---

## Repository Structure (High-Level)

This project follows a standard **dbt (data build tool)** structure.

Here is a brief explanation of the main folders you’ll see:

- **models/**  
  Contains all SQL models used to transform data (this is where most of the work happens)

- **macros/**  
  Reusable SQL logic to avoid duplication

- **seeds/**  
  Static reference data loaded into the warehouse

- **snapshots/**  
  Used to track changes in data over time

- **tests/**  
  Data quality tests to ensure accuracy and consistency

- **dbt_project.yml**  
  Configuration file that defines how dbt runs this project

If you’re non-technical, you don’t need to understand every folder — just know that this structure is how modern analytics teams organize transformation logic.

---

## The Modeling Approach

The core work happens inside the **models/** folder.

Inside it, you’ll find the following key layers:

### 1. Staging Models (`staging/`)

Staging models are the **first transformation step**.

Their purpose is **not analysis**.

They are used to:
- clean column names
- standardize data types
- normalize values (e.g. statuses, timestamps)
- remove obvious inconsistencies

Think of staging as **preparing the data**, not answering questions.

---

### 2. Fact and Dimension Models (`facts/` and `dimensions/`)

After staging, the cleaned data is reshaped into **analytical tables**.

#### Dimension Tables
Dimensions describe entities, such as:
- users
- wallets
- assets
- datasets

They answer questions like:
> *Who is involved? What is this?*

#### Fact Tables
Facts store measurable events, such as:
- transactions
- trades
- licenses
- revenue

They answer questions like:
> *What happened? When did it happen? How much?*

These tables are designed to be:
- easy to query
- consistent
- reliable for dashboards and reporting

---

## Why This Project Matters

In real-world analytics work:
- data is rarely clean
- schemas are rarely designed for analysis
- definitions often change or are unclear

This project demonstrates how to:
- handle messy source data
- apply structured data modeling
- create a reliable analytics layer
- reduce confusion and duplicated logic

It reflects how data teams actually work in production environments.

---

## Who This Project Is For

- Recruiters and hiring managers evaluating data skills
- Analysts learning data modeling best practices
- Anyone curious about how raw data becomes usable analytics

---

## Tools Used

- SQL
- dbt
- Git & GitHub
- VS Code

---

## Final Note

This repository focuses on **data modeling**, not dashboards or visualizations.

The goal is to show how **good analytics starts with good data structure**.
































