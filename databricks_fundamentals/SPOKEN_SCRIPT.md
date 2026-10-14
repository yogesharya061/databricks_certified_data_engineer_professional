# Databricks Professional Course — Video 1 Spoken Script

**Yogesshh Arya | Databricks Architecture, Unity Catalog and Your First Retail Lakehouse**

Adapted from your uploaded `RECORDING_GUIDE.md`.

## How to use this script

Read the ordinary paragraphs aloud. Text inside **[square brackets]** is a production cue, not dialogue. Keep this script on your second screen and the original notebook on the recording screen. Cell numbers refer to the notebook in your recording kit; this script explains that implementation rather than replacing its code.

The chapter timings retain the guide’s 85-minute recording plan. They include running cells, highlighting code, inspecting results and giving viewers time to follow. Reading the dialogue alone will be shorter. Rehearse and adjust the final timestamps; do not stretch pauses to reach a target duration.

Before recording, rehearse Cells 01–11 and all three failure/repair pairs. Read success lines only after the corresponding results appear. Use a suitable demo environment as described in the original guide. Do not display credentials or private data. Publish the notebook before promising a download link; the optional download sentence below is marked accordingly.

---

## Chapter 1 — The business problem and what we will build
**Planned recording window: 00:00–05:00**

### Opening hook

**[CAMERA: Look into the lens. Start directly; use a short channel animation only after the hook.]**

Imagine your manager asks you a simple question: “How much were yesterday’s completed orders worth?”

You open the order file, write a query, add up the amounts, and send the number.

But there’s a problem. One order appears twice. A few orders were cancelled. Another record has a negative quantity.

Your query runs successfully. There’s no red error message. But the number you sent is wrong.

That’s the kind of problem we’re going to solve today.

Hi everyone, I’m Yogesshh Arya. Welcome to the first video in our Databricks Data Engineer Professional preparation series.

We’ll start with a small retail example and build a working lakehouse, step by step. Along the way, I’ll explain catalogs, schemas, volumes, tables and compute using the objects we actually create.

And towards the end, we’ll deliberately introduce a few mistakes and fix them together. Because being able to run a notebook is useful, but being able to explain and troubleshoot it is what makes you confident.

### Meet our example business

**[SCREEN: Open ARCHITECTURE.html. Point to the source-data card.]**

Our example company is called Arya Retail. Think of it as a small online shop.

It sends us three extracts: customers, products and orders.

The customer data tells us who placed an order. Product data tells us what the shop sells. Order data tells us which product was ordered, the quantity, the unit price and whether the order was completed or cancelled.

For today’s lesson, our business rule is simple: completed order value is quantity multiplied by unit price, and cancelled orders must be excluded.

So, if someone completes an order for two items at one hundred each, that order contributes two hundred to our report.

If they cancel the order, the record still exists, but it contributes nothing to this particular measure.

Notice that I’m saying “completed order value.” We haven’t included payments, returns, discounts or tax. So we shouldn’t label this number as money collected or accounting revenue.

That may sound like a small wording detail, but it helps everyone understand what the report actually means.

### Set expectations

**[SCREEN: Point to the pipeline and outcome cards.]**

We’ll first preserve the incoming records. Then we’ll separate invalid records, remove an exact duplicate, and produce a report by day and by customer.

Here’s a question for you before we start.

If an order has an invalid quantity, should we simply delete it and move on?

**[PAUSE: Three seconds.]**

In this project, we’ll keep it separately with an explanation. That way, we can show what went wrong and investigate it later.

By the end, you should be able to explain where the source files live, where the tables live, what runs the code, and how every input row is accounted for.

You don’t need to memorise all the syntax today. Focus on the purpose of each step, and pause the video whenever you want to try it yourself.

This is an independent preparation series. The practice scenarios are our own learning examples, not actual exam questions.

All right, let’s understand the architecture before we start creating things.

---

## Chapter 2 — Concepts and architecture
**Planned recording window: 05:00–23:00**

### Storage, compute and governance

**[SCREEN: Keep the architecture visual open. Highlight one concept at a time.]**

There are three questions I want you to keep asking throughout this course.

Where is my data stored?

What executes my code?

And who is allowed to access the data?

Storage answers the first question. Compute answers the second. Governance and permissions help answer the third.

Let’s use a warehouse example for a moment.

The storage area holds the goods. Workers perform tasks on those goods. An inventory system tells you what exists and where to find it, while access rules control who can do what.

In our Databricks project, files and table data live in storage. Compute processes them. Unity Catalog helps organise and govern our data objects.

The notebook is where we write and run the instructions.

**[SCREEN: Switch to Databricks. Point to Workspace, Catalog and the notebook compute selector.]**

This distinction matters. Saving a notebook does not mean that the notebook itself is the storage location for the Delta tables it creates.

Think of a recipe and the meal prepared from it. The recipe tells you what to do; it isn’t where the finished meal is stored.

Likewise, our code describes the processing, and the tables are persisted separately.

**[ACTION: Briefly show the notebook, then Catalog Explorer. Return to the visual.]**

### Why Bronze, Silver and Gold?

You’ll hear these three names often: Bronze, Silver and Gold.

Let’s follow one order through them.

Suppose the source says that order O0001 contains one item at one hundred. That record arrives in a JSON file.

The landing area holds that file.

Bronze keeps the source values with minimal changes, along with useful information such as where the record came from and when we loaded it.

Silver is where we apply our validation rules. Is the quantity valid? Can we interpret the date? Is the customer identifier present? Have we received the exact same record twice?

Gold presents data for a business purpose. In our case, that means completed order value by date and by customer.

**[SCREEN: Trace the path, then point to the quarantine branch.]**

You can think of it as receiving a delivery, inspecting the contents, and preparing the figures for the shop owner.

If something is wrong with a delivered item, we record the issue. We don’t quietly pretend that it never arrived.

The same idea applies to our rejected records.

Now, Bronze, Silver and Gold describe responsibilities. Naming a schema “Silver” doesn’t automatically clean its data. We still need code that implements the rules.

Also, you don’t have to use exactly three schemas in every real project. We’re using that structure here because it makes the boundaries easy to see.

One more detail: our notebook will write several tables separately. Those writes do not turn the entire notebook into one single transaction across every layer.

For this first lesson, we’re building a small, repeatable snapshot. We’ll introduce more advanced production behaviour later.

### Catalog, schema and table: read the address

**[SCREEN: Display `retail_de_course.v01_bronze.orders` in a large markdown cell.]**

Let’s read this name from left to right.

`retail_de_course` is our catalog.

`v01_bronze` is the schema inside that catalog.

And `orders` is the table inside that schema.

It’s a three-part address: catalog, schema, table.

An everyday comparison is a building, a floor and a room. Saying “room twelve” might work when everybody knows which building and floor you mean. But the complete address is much clearer when there’s any ambiguity.

That’s why we’ll use fully qualified table names in the important parts of this notebook.

A schema is also called a database in Spark SQL terminology. Here, it’s the middle level of our object name.

The `v01` prefix just means video one. It keeps this lesson’s objects easy to recognise.

Later, I’ll deliberately change the current schema and run an incomplete table name. You’ll see how a table can exist and still not be found by that particular query.

### A volume is for files

**[SCREEN: Show `/Volumes/retail_de_course/v01_bronze/landing/video01/input/orders.json`.]**

This is a different kind of address.

It begins with `/Volumes`, followed by the catalog, schema and volume name. After that, we have directories and a filename.

Our volume is called `landing`. The JSON file lives inside it.

Here’s a common point of confusion: a JSON file can contain structured records, but that doesn’t automatically make it a registered table.

We use the volume to work with the file. Then we read its records and write them into a separately managed table.

So when I want to inspect the incoming file, I use a file path. When I want to query the Bronze orders table, I use its three-part table name.

Those are two different objects with two different jobs.

We will not create a registered table on a path inside our landing volume. The source files and managed table storage stay separate.

### Managed and external tables

**[SCREEN: Show the comparison table from the recording guide.]**

Now let’s discuss managed and external tables.

For a managed table, Databricks manages the storage lifecycle and layout associated with the table. In our example, we supply a table name and let the configured managed storage handle its location.

For an external table, we explicitly refer to a cloud storage location that has been configured for access.

Imagine another system already owns a collection of data in a cloud directory, and there’s a concrete reason that directory must remain under that arrangement. That’s a situation where you would investigate external tables and the relevant storage permissions.

For our learning project, we don’t have that requirement. Managed tables keep the setup straightforward.

Managed data still lives in cloud storage. It doesn’t mean that the table is stored in the notebook or permanently in memory.

Also, managed versus external is different from Delta versus another data format. A managed table can be Delta, and an external table can also be Delta.

One choice concerns how the data is represented. The other concerns how its storage is managed.

**[SCREEN: Point to the optional external-table SQL in the guide. Do not execute it.]**

Notice the `LOCATION` clause in this example.

That cloud path is a template, not a real storage account. Before using it, we would need an appropriate external location, permissions, and either an existing Delta dataset or a proper table definition.

We’re explaining the choice today. We aren’t setting up external cloud storage in this lesson.

Another useful distinction: dropping an external table’s registration does not delete the underlying cloud files. Managed objects have their own managed deletion and recovery lifecycle, so don’t assume that dropping a managed table instantly erases every byte either.

The important habit is to understand the lifecycle before performing a destructive operation.

### Which compute do we need?

**[SCREEN: Show the notebook compute selector.]**

For our mixed Python and SQL development, I’m using compatible notebook compute. This guide’s default-storage path uses serverless notebook compute.

When we query the reporting table from the SQL editor, we’ll use a SQL warehouse, if one is available in the demo environment.

For scheduled pipelines, we’ll work with Lakeflow Jobs in a later video.

Serverless means much of the infrastructure setup is handled for us. It doesn’t mean that there are no limits, no permissions to consider, or no cost decisions.

If your screen looks slightly different, that may be due to your workspace edition or configuration. Follow the setup notes that match your environment.

For this lesson, avoid spending time creating a custom cluster that the exercise doesn’t require.

**[ACTION: Briefly show the workspace folder, imported notebook and SQL editor navigation.]**

All right. We know what the objects mean. Let’s create them and see how they work together.

---

## Chapter 3 — Build the retail lakehouse
**Planned recording window: 23:00–63:00**

### Cell 01 — Configuration and current context

**[SCREEN: Open Cell 01. Highlight CATALOG, CREATE_CATALOG and the schema variables.]**

We’ll start by putting the configuration in one place.

Our catalog name is `retail_de_course`. Then we have the three schemas for this video: Bronze, Silver and Gold.

The `CREATE_CATALOG` flag controls whether the notebook should attempt to create the catalog.

If you’re using a catalog that an administrator has already prepared, change the catalog variable to that name and set the flag to `False`.

This is much easier to maintain than changing a catalog name in twenty different queries.

The small helper function below builds a fully qualified table name for us. That keeps the later code easier to read.

And the validation on the catalog name makes sure that our simple naming convention is followed.

Before creating anything, let’s see our current context and the catalogs available to us.

**[RUN CELL 01. Pause for the output. Highlight current catalog and schema.]**

These values tell us where unqualified object names would be resolved in this session.

The catalog list is useful, but seeing a catalog is not the same as having permission to create tables inside it. We still need the appropriate access for what we’re about to do.

**[IF THE TARGET CATALOG OR REQUIRED ACCESS IS MISSING: Stop the demo and use the environment fallback in the recording guide. Do not read the next success line.]**

### Cell 02 — Create the catalog and schemas

**[SCREEN: Highlight CREATE CATALOG, CREATE SCHEMA and IF NOT EXISTS.]**

Now we’ll create our catalog, if that option is enabled, and our three schemas.

Notice `IF NOT EXISTS`.

It means that if the object is already there, this statement doesn’t try to recreate it. That’s useful when you repeat a tutorial or restart after a break.

I’ve also added short descriptions. They may look unimportant in a tiny exercise, but imagine opening a workspace containing hundreds of objects with no explanation of what they’re for.

A useful description saves the next person from guessing.

**[RUN CELL 02. Open Catalog Explorer, refresh and expand the catalog.]**

Here are our three schemas. At this stage, we’ve created the structure. We haven’t cleaned or loaded any data yet.

**[OPTIONAL LINE, ONLY IF YOU REHEARSED IN THE SAME CATALOG:]**

I ran this setup during rehearsal, so some objects already existed. That’s why using repeatable setup statements is helpful.

**[IF CATALOG CREATION FAILS, SAY:]**

This error is about our environment’s catalog or storage setup. It doesn’t mean we need a different transformation query. I’ll use a permitted pre-created catalog, or the supported default-storage setup in this workspace, then continue with the same isolated lesson schemas.

### Cell 03 — Create the landing volume

**[SCREEN: Highlight CREATE VOLUME and INPUT_DIR.]**

Next, we’ll create the managed volume called `landing`.

Inside it, we’ll use a `video01/input` directory for our three source files.

Keeping the path in a variable helps us avoid small typing mistakes across different cells.

We’ll use those variables when writing and reading the files.

**[RUN CELL 03. Show the volume listing and printed path.]**

Now we have a governed place for our input files.

Notice that we created the volume as a catalog object first. Creating an ordinary folder with a similar name would not be the same operation.

### Cell 04 — Generate realistic but small source data

**[SCREEN: Scroll through the customer, product and order generators.]**

We’ll generate all the data ourselves, so you can reproduce the example without needing access to a company’s systems.

There are ten customers and five products. We then create twenty distinct valid orders across two dates.

Some of those valid orders are cancelled. Remember, a cancelled order can be a perfectly valid record even though it’s excluded from our completed-order metric.

Now look at the final three lines that add records.

We copy one order exactly. That creates our duplicate.

Then we create an order with a negative quantity.

And finally, we create an order with a missing customer identifier.

So we expect twenty-three raw order rows.

**[PAUSE. Point to the quantities and customer fields.]**

Why introduce problems deliberately?

Because a pipeline that only sees perfect sample data doesn’t teach us much about handling the data we actually receive in projects.

This small dataset lets us see the problem and the correction without searching through millions of rows.

**[RUN CELL 04. Show the three counts, file list and JSON preview.]**

Here are our files, and here’s what an order record looks like.

Each line contains one JSON object. That’s why this format is often described as newline-delimited JSON.

We’re writing the same fixed source filenames for this lesson. The overwrite option replaces these demo files on a rerun. For a production arrival process, we’d need to think about retaining incoming files and incremental processing. We’ll get to that later.

Before continuing, try to predict how many valid unique orders should remain after removing the two invalid records and the extra duplicate.

**[PAUSE FOR VIEWERS. Leave the count on screen.]**

### Cell 05 — Read the files with explicit schemas

**[SCREEN: Highlight source_schemas and the read operation.]**

Now we’ll read those files.

A schema describes the fields and their data types. Here, we’ve deliberately defined the order’s source fields as strings.

For example, the quantity arrives as text. We’ll convert and validate it in the Silver step.

Using an explicit landing schema makes the behaviour of this exercise predictable. We can inspect the original values before deciding whether they’re acceptable.

There are also two different kinds of problems to keep separate.

A broken JSON structure is a parsing problem. A correctly formed JSON record with quantity minus two is a business-data problem.

The `FAILFAST` setting in this controlled example catches malformed input during parsing. It doesn’t implement our business rules for us.

**[RUN CELL 05. Show the records, then printSchema output.]**

The records are now available as DataFrames. We can inspect them, but we haven’t yet saved them as the registered Bronze tables.

That’s our next step.

### Cell 06 — Save the Bronze Delta tables

**[SCREEN: Highlight the two metadata columns and the write operation.]**

Before writing the data, we add two pieces of information.

`_source_file` tells us which source file these records came from.

`_ingested_at` tells us when this load happened.

Imagine an analyst later asks, “Why is this order in the table?” Those fields give us a starting point for investigation.

In this example, each dataset comes from one known file, so we can assign that source path directly. A larger multi-file ingestion process would need to capture provenance appropriately for each file.

Now look at the write.

We choose the Delta format and save the data using the table name.

We aren’t supplying a physical table path or a `LOCATION` here. The configured managed storage takes care of that.

**[RUN CELL 06. Inspect SHOW TABLES and DESCRIBE TABLE EXTENDED.]**

We now have registered Bronze tables. The orders table contains all twenty-three rows.

That includes the duplicate and the invalid business values. We haven’t silently removed them at ingestion.

**[SCREEN: Find the table type and provider in the details. Read them only after confirming the actual output.]**

Here we can confirm the managed table type and the Delta provider.

The overwrite mode makes this fixed exercise repeatable. Each run replaces our demo snapshot. It is not our final production incremental-ingestion design.

### Cell 07 — Prepare customer and product references

**[SCREEN: Highlight the customer/product SQL and key assertions.]**

Before cleaning the orders, we’ll prepare the small customer and product reference tables.

The customer table provides names and cities. The product table provides product details, with a numeric price type.

We also check the reference keys.

Let’s say customer C001 accidentally appears twice in the customer table. If we join one order to both customer rows, that one order can become two rows in the result.

The query might run successfully, but our total could increase incorrectly.

That’s why checking the number of rows alone is not enough. We also check that the key is unique and not missing.

**[RUN CELL 07. Show the successful checks.]**

Our controlled reference data passes those checks. Now we can move on to the order validation.

### Cell 08 — Validate and quarantine

**[SCREEN: Show the typed CTE first. Avoid displaying the whole cell at tiny font size.]**

This cell looks longer, so let’s break it into three parts.

First, we convert source values into useful types.

Second, we decide whether each record satisfies our rules.

Third, we separate accepted and rejected rows.

Here we use `TRY_CAST` for the date, quantity and price.

Think about a quantity containing the word “two” rather than the digit two. For this conversion, that would not be a valid integer representation. `TRY_CAST` lets us detect the failed conversion as a null instead of letting that value interrupt this validation step.

We can then mark the row invalid and investigate it.

**[SCREEN: Highlight the is_valid expression.]**

Our rules check that important identifiers are present, the date can be interpreted, quantity and price are positive, and status is one of our expected values.

You’ll also notice `COALESCE` around the condition.

In SQL, some comparisons involving null produce an unknown result rather than a simple true or false. Here we treat that unknown validation result as false, so it enters the invalid-data branch.

**[SCREEN: Highlight the quarantine query and rejection_reason.]**

Rejected rows go into `orders_quarantine` with a reason.

Think of quarantine as an investigation tray. The record didn’t pass our current rules, but we still have it and can explain why it was held back.

**[RUN CELL 08. Show the quarantine rows.]**

Here is O9001 with an invalid quantity.

And here is O9002 with a missing customer identifier.

This is much more useful than just reporting, “Two rows disappeared.”

**[SCREEN: Return to SELECT DISTINCT in the accepted-data query.]**

For the valid rows, we also remove the exact duplicate across the business fields.

Notice that we aren’t including the ingestion timestamp in that comparison. Our goal is to compare the business record.

But don’t take away the idea that `DISTINCT` solves every duplicate problem.

If the same order identifier appears once with quantity two and later with quantity three, those are different versions. We would need a rule for deciding which version to use. That belongs to our later incremental and CDC lessons.

Today, the duplicate really is an exact copy, so this simple approach matches our example.

**[SCREEN: Show the successful counts/assertions.]**

We have twenty valid unique orders and two rejected rows. One extra exact duplicate has been removed.

These checks cover our designed example. A production quality framework would also need rules for things like blank identifiers, missing references and unexpected combinations of values.

### Cell 09 — Turn clean records into a business answer

**[SCREEN: Highlight the daily_sales query, starting with FROM and WHERE.]**

Now we’re ready to answer the original business question.

Start with the source of this query: our Silver orders table.

Then look at the filter: only completed orders.

Finally, we group by order date, count the completed orders and sum their values.

A valid cancelled order stays in Silver because it’s part of the order history. It simply doesn’t qualify for this Gold metric.

That distinction is useful: “valid record” and “included in this report” are not always the same thing.

**[RUN CELL 09. Show daily_sales.]**

For the first of September, we have eight completed orders with a value of four thousand one hundred.

For the second of September, we have eight completed orders with a value of three thousand seven hundred.

Together, that’s sixteen completed orders and seven thousand eight hundred in completed order value.

**[SCREEN: Show customer_sales.]**

We also create a customer-level result.

Here we join the orders to the customer reference so that the result contains a readable name and city.

We group using the customer identifier as well as the descriptive fields. Two customers can share the same name, so the name alone isn’t a safe identity.

The business can now ask either “How did each day perform?” or “What did each customer contribute?” without rebuilding the cleaning rules each time.

### Cell 10 — Make the result understandable and accessible

**[SCREEN: Highlight the table comments. Run Cell 10.]**

We’re going to add one small but useful improvement: descriptions.

Look at the comment for `daily_sales`. It says that cancelled orders are excluded and that this is completed order value, not settled revenue.

Imagine that someone finds this table six months later. The name `daily_sales` on its own leaves room for interpretation. The description helps them understand its intended use.

**[ACTION: Open Catalog Explorer → the Gold schema → daily_sales. Show the comment and schema.]**

Now let’s use the result outside the notebook.

**[ACTION: Open SQL editor, select an available warehouse and paste the exact query printed by Cell 10. If using a different catalog, use that printed name.]**

Here we’re querying the registered table by its full name.

**[RUN THE SELECT. Wait for the two rows.]**

We get the same results. The table isn’t tied to this notebook’s Python variables. Another authorised SQL consumer can query the saved data.

**[OPTIONAL: Add a date-versus-value chart if the UI supports it. Keep this brief.]**

A quick chart makes the difference between the two days easier to see. We’ll explore reporting features more later; our focus today is the data behind the chart.

**[ALTERNATIVE, ONLY IF A SQL WAREHOUSE IS UNAVAILABLE:]**

The separate SQL warehouse demonstration isn’t available in this environment right now. I’ll show the same fully qualified query through the notebook. That validates the query, but it isn’t a demonstration of execution on a separate SQL warehouse.

### Cell 11 — Prove that the numbers reconcile

**[SCREEN: Return to the notebook. Highlight the reconciliation assertions.]**

We have a result, but we still need to prove that it makes sense.

Let’s account for all twenty-three source rows.

Twenty remain as valid unique orders.

Two are in quarantine.

One is the extra exact duplicate that we removed.

Twenty plus two plus one equals twenty-three.

Nothing is unexplained.

**[RUN CELL 11. Point to PASS and each count.]**

The checks also confirm that sixteen orders are completed and that the total completed order value is seven thousand eight hundred.

We calculate an expected total directly from our original controlled fixture and compare it with the reporting output. We also check that the daily and customer totals agree.

Why bother with both counts and amounts?

Because two results can contain the same number of rows but different values. And two reports can look convincing while applying slightly different rules.

These assertions make our expectations explicit.

**[ACTION: Re-run Cell 09, then Cell 11.]**

The business totals remain the same after rebuilding Gold.

That’s expected because this lesson replaces the output snapshot. It doesn’t yet demonstrate exactly-once streaming or an incremental merge pipeline.

We’ve now completed the happy path. Let’s see what happens when we introduce a few controlled mistakes.

---

## Chapter 4 — Break it, understand it, fix it
**Planned recording window: 63:00–77:00**

### Failure 1 — The table exists, but the query cannot find it

**[SCREEN: Cell 12. Set RUN_NAMESPACE_FAILURE=True. Run only this cell.]**

I’m deliberately changing our current schema to Bronze, then querying `daily_sales` without its complete name.

Let’s run it.

**[WAIT FOR THE EXPECTED ERROR. Highlight only the relevant error category.]**

We’ve got a table-not-found error.

Now, we know that we created `daily_sales`. So should we immediately create it again?

**[PAUSE.]**

First, let’s check where this query is looking.

**[RUN CELL 13. Show current schema, then the fully qualified successful query.]**

The current schema is Bronze. But our report is in Gold.

That’s like asking for a room on the wrong floor. The room may exist elsewhere in the building, but the incomplete address points us in the wrong direction.

Using the complete catalog, schema and table name resolves the correct object.

We didn’t need to recreate the table or broaden permissions to fix this particular error.

In another situation, access restrictions can affect what you can see. The useful habit is to inspect the context and evidence before choosing the repair.

**[RESET RUN_NAMESPACE_FAILURE=False.]**

### Failure 2 — A small typo in a volume path

**[SCREEN: Cell 14. Set RUN_PATH_FAILURE=True and highlight landing_typo.]**

For the next example, I’ve changed the volume name in our path from `landing` to `landing_typo`.

Everything else looks similar, which is why this kind of error is easy to miss.

**[RUN CELL 14. Show the path/volume error.]**

Let’s compare the path with the registered object instead of guessing.

**[RUN CELL 15. Show the actual volume name and successful directory listing.]**

Here is the real volume: `landing`.

Once the path matches that object, the files can be listed again.

Remember the structure: slash Volumes, catalog, schema, volume, then the directories and file.

A table name and a volume path are different kinds of reference. Creating a table called `landing_typo` would not repair this path.

**[RESET RUN_PATH_FAILURE=False.]**

### Failure 3 — Correct SQL, incorrect business answer

**[SCREEN: Cell 16. Set RUN_METRIC_FAILURE=True.]**

Our third mistake is a little more subtle.

This query reads the Silver table and calculates the order count and total amount. The SQL itself is valid.

Can you spot what’s missing?

**[PAUSE FOR FIVE SECONDS. Point at the absence of a status filter.]**

We forgot to restrict the result to completed orders.

**[RUN CELL 16. Show the count of 20 and the intentional assertion failure.]**

The query runs, but the count is twenty instead of sixteen.

The assertion catches the difference because we defined what the result should mean.

This is exactly the problem from the opening of the video. Successful execution isn’t enough to prove that a report is correct.

**[RUN CELL 17. Highlight WHERE status = 'COMPLETED'.]**

Adding the business filter brings us back to sixteen completed orders and the expected value.

Now let’s rerun the full reconciliation check.

**[RUN CELL 11. Confirm PASS. RESET RUN_METRIC_FAILURE=False.]**

The numbers agree again.

When you build a report, write down its definition before writing the aggregation. Then turn that definition into checks wherever you can.

### Common environment problems

**[SCREEN: Show the troubleshooting table from the guide. These are explanation-only unless encountered.]**

There are a few other problems you might encounter while following along.

If catalog creation reports missing managed storage, check the workspace’s storage setup. You may need the supported default-storage option or an administrator-prepared catalog.

If you get a permission error, identify the operation that was denied. Being able to view a table doesn’t automatically give you permission to create a schema or write a file.

Ask for the specific access needed rather than treating administrator rights as the default fix.

If code fails immediately after pasting it, check the cell language. Our notebook uses Python cells, with SQL passed through `spark.sql`. A Python block pasted directly into SQL editor won’t run as SQL.

And if the final counts differ, first check whether you changed the fixture or ran only part of the notebook. Start with the controlled input and execute the success cells in order.

My approach is to read the error, inspect the relevant context, change one thing, and verify the result.

That keeps troubleshooting focused and makes the explanation easier to follow.

---

## Chapter 5 — Exam decision points, recap and next steps
**Planned recording window: 77:00–85:00**

### Apply the ideas to short scenarios

**[SCREEN: Show one scenario at a time. These are open discussion prompts, not a multiple-choice quiz.]**

Before we finish, let’s check whether we can use these concepts without looking at the code.

I’ll give you six short scenarios. Pause after each one if you want to think about it.

**[SCENARIO 1 ON SCREEN: “Where should raw JSON files go before processing?”]**

A team receives JSON files and wants to keep the original files before parsing them. What object would we use in our design?

**[PAUSE.]**

A volume for the files. After processing their records, we can write separately registered tables. Structured content inside a file doesn’t automatically turn the file into a table.

**[SCENARIO 2: “Managed or external for our new course tables?”]**

We’re creating new tables and have no requirement to manually control their physical cloud paths. Where would we start?

**[PAUSE.]**

Managed tables. If an external-storage requirement appears, we evaluate it specifically. We don’t choose external simply because the project is important or intended for production.

**[SCENARIO 3: “What happens to files when an external table is dropped?”]**

Someone drops the registration for an external table. Should they assume the cloud files have been removed?

**[PAUSE.]**

No. Removing the external-table object does not delete those underlying files. That’s why storage lifecycle matters when designing retention and cleanup.

**[SCENARIO 4: “What should happen to an invalid quantity?”]**

An order contains a negative quantity under our lesson’s rules. Should we remove every trace of it?

**[PAUSE.]**

We preserve the source and keep the rejected record with a reason. We can then explain the discrepancy and investigate or reprocess it later.

**[SCENARIO 5: “A Gold table cannot be found from Bronze.”]**

We know a reporting table exists, but an unqualified query from another schema fails. What do we inspect first?

**[PAUSE.]**

The current catalog and schema, then the fully qualified name. We establish what the query is trying to resolve before deciding on a change.

**[SCENARIO 6: “An analyst needs SQL access to the published report.”]**

Which compute interface should we evaluate for an analyst running SQL against our reporting tables?

**[PAUSE.]**

A SQL warehouse. The exact configuration depends on the workload. Our Python development notebook and that SQL workload don’t have identical execution requirements.

If you could explain those answers in your own words, you’ve understood the main decisions from today’s lesson.

### Recap the project

**[SCREEN: Return to Catalog Explorer, then the PASS output and daily results.]**

Let’s come back to where we started.

Arya Retail needed a reliable daily completed-order figure.

We created a catalog, three schemas and a landing volume. We generated our source files, loaded them into managed Bronze Delta tables, and applied our validation rules in Silver.

Then we created Gold reports for the business.

We can account for all twenty-three raw rows: twenty valid unique orders, two quarantined rows and one extra duplicate.

Sixteen of those valid orders are completed, giving a total completed order value of seven thousand eight hundred.

We also saw three different problems: an object-name problem, a file-path problem, and a business-rule problem.

Each needed a different repair.

This gives us the foundation for the rest of the course. We’ll extend it with stronger transformations, incremental ingestion, CDC, deployment and monitoring.

### Give viewers a small exercise

**[SCREEN: Display the exercise. Keep the original notebook untouched during the explanation.]**

Here’s a small exercise before the next lesson.

Add one new completed order with a unique order identifier, an existing customer and product identifier, quantity two, and unit price one hundred.

Before running anything, predict what should change.

**[PAUSE.]**

The completed order count should become seventeen, and the completed order value should become eight thousand.

The raw count should become twenty-four, and the valid unique count should become twenty-one. The existing two rejected records and one extra duplicate remain unchanged.

There’s one implementation detail to watch: our original expected-value calculation selects the first twenty baseline orders. When you extend the fixture, update the expected-data selection as well as the count assertions so your new order is included.

Don’t simply remove a failing assertion. Decide what the new expected result is, then make the test express that expectation.

Keep your exercise as a separate version, or restore the original sample before following the next lesson.

### Closing

**[CAMERA: Return to presenter.]**

In the next video, we’ll work on the transformation code itself: joins, window functions, nested data, and reusable Python components.

If something in today’s lesson was confusing, tell me which step and what result you saw. Include the error category if there was one, but please don’t post credentials or private data.

**[OPTIONAL — SAY ONLY AFTER THE NOTEBOOK HAS BEEN PUBLISHED AND LINKED:]**

You’ll find the notebook and setup instructions in the description, so you can build the same project yourself.

If you’d like to follow the full series, subscribe to the channel. And try the exercise before moving on; even a small change is a good way to see whether the logic is clear.

Thanks for learning with me. I’m Yogesshh Arya.

Build. Understand. Apply.

See you in the next video.

---

## Presenter notes — not for narration

- Record in chapter-sized takes. A brief pause between paragraphs makes editing easier.
- Keep your voice conversational. Emphasise the problem and the result rather than every punctuation mark in the code.
- During execution, highlight the next line to discuss; cut long waits rather than filling them with repeated explanations.
- If a result differs, inspect it before reading the scripted expected result. The source guide identifies these as expected outputs, not proof of a successful run in your environment.
- Rehearse the original notebook before recording. This adaptation did not execute it in Databricks.
- Use one analogy at a time, then return to the exact technical terms.
- Keep the claim boundaries: full-refresh teaching snapshot; no production CDC, end-to-end exactly-once guarantee, performance benchmark, or complete data-quality framework.
- If an output or feature is unavailable, use the marked alternative narration. Do not describe an unperformed action as completed.
- The original guide’s environment and usage notes remain applicable. This script adapts the supplied material; it is not a fresh product-availability audit.
- Match the published chapter timestamps to the actual edited video. The 85-minute plan includes implementation and viewer-follow-along time, not 85 minutes of continuous scripted speech.
