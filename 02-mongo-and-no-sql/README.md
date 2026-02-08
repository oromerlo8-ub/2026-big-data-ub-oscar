# MongoDB Practice: Movies Dataset

## Setup

From your local git repository:

```bash
cd 02-mongo-and-no-sql
```

### Start MongoDB

Review the `compose.yaml` file.

```bash
docker compose up -d
```

This starts a MongoDB 7 instance. The `seed-data/` directory is mounted inside the
container at `/seed-data/`.

### Connect with mongosh

Validate you can connect to mongo with:

```bash
docker exec -it mongodb mongosh
```

To exit press `Ctrl+D`.

---

## Exercises

### Exercise 1: Load the dataset

From inside the container, use `mongoimport` to load the movies dataset:

```bash
docker exec mongodb mongoimport --db entertainment --collection movies --drop --file /seed-data/movies.json
```

How many documents where imported? Read the log output!

Now connect with `mongosh` and verify:

```bash
docker exec -it mongodb mongosh entertainment
```

```javascript
db.movies.countDocuments()
```
You should see **35**.

### Exploring the data

Before writing queries, take a moment to understand the document structure:

```javascript
db.movies.findOne()
```

Each movie document has: `title`, `year`, `director`, `genres` (array), `cast` (array of
objects with `name` and `role`), `ratings` (object with `imdb` and `rotten_tomatoes`),
`reviews` (array of objects), `box_office`, `runtime`, and `country`.

### Exercise 2: Basic find queries

**Q1.** Find all movies directed by Christopher Nolan.

<details>
<summary><b>Answer</b></summary>

```javascript
{'director': 'Christopher Nolan'}
```
</details>

**Q1.5** How many are there?

<details>
<summary><b>Hint</b></summary>

```javascript
Use .count()
```
</details>

**Q2.** Find all movies released in 1994. Return only the `title` and `year` fields
(exclude `_id`).

<details>
<summary><b>Hint</b></summary>

```javascript
{'year': 1994}, {year: 1, _id: 0}
```
</details>

**Q3.** Find all movies with a runtime of less than 100 minutes. (`$lt`)

<details>
<summary><b>Hint</b></summary>

```javascript
{runtime: {$lt: 100}}
```
</details>

### Exercise 3: Query operators

**Q4.** Find all movies released between 2000 and 2010 (inclusive).

<details>
<summary><b>Hint</b></summary>

```javascript
Use `$gte` and `$lte`.
```
</details>

**Q5.** Find all movies from either "Japan", "South Korea", or "Brazil".

<details>
<summary><b>Hint</b></summary>

```javascript
Use `$in`.
```
</details>

### Exercise 4: Querying arrays and nested documents

**Q6.** Find all movies that have "Sci-Fi" in their `genres` array.

<details>
<summary><b>Hint</b></summary>

```javascript
Use `$in`.
```
</details>

**Q7.** Find all movies where Leonardo DiCaprio is in the cast.

<details>
<summary><b>Hint 1</b></summary>
use dot notation on the `cast.name` field.
</details>

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.find({'cast.name': 'Leonardo DiCaprio'})
```
</details>

**Q8.** Find all movies with an IMDb rating greater than 8.5. The field is `ratings.imdb`.

<details>
<summary><b>Hint</b></summary>

```javascript
{'ratings.imdb': {???}}
```
</details>

<details>
<summary><b>Hint2</b></summary>

```javascript
{$gte: xxx}
```
</details>

### Exercise 5: Insert, update, and delete

**Q9.** Insert a new movie document of your choice. Make sure it follows the same
structure as the existing documents (include all fields).

Example:
```json
    title: "",
    year: zzzz,
    director: "",
    genres: ["", ""],
    cast: [
      { name: "", role: "" },
      { name: "", role: "" },
      { name: "", role: "" }
    ],
    ratings: { imdb: z.z, rotten_tomatoes: zz },
    reviews: [
      { user: "", score: 0, text: "" },
      { user: "", score: 0, text: "" }
    ],
    box_office: ,
    runtime: ,
    country: ""
```

<details>
<summary><b>Hint</b></summary>

```javascript
db.movies.insertOne({})
```
</details>

Verify the insert:

```javascript
db.movies.countDocuments()
```

Search for it:

```javascript
db.movies.findOne({title: "The title you have added"})
```

**Q10.** Update the movie you just inserted: change its `box_office` value to a new number.
Use `updateOne`.

<details>
<summary><b>Hint</b></summary>

```javascript
{$set :{box_office: 51525172}
```
</details>

**Q11.** Delete the movie you inserted. Use `deleteOne`.

### Exercise 6: Aggregation pipeline

An aggregation pipeline is a sequence of stages that process documents step by step.
Field references inside pipeline stages are strings prefixed with `$`, e.g. `"$fieldname"`.

**Q12.** Order movies by box office revenue. User `$sort` and `db.movies.aggregate`

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.aggregate({ $sort: { box_office: -1 } })
```
</details>

**Q13.** Find the top 5 movies by imdb. Use `$sort` and `$limit`.

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.aggregate([
  { $sort: { "ratings.imdb": -1 } },
  { $limit: 5 }
])
```
</details>

**Q14.** Count how many movies exist per country. Use `$group` with `$sum`.

<details>
<summary><b>Hint</b></summary>

```javascript
{ $group: { _id: "$country", count: { $sum: 1 } } }

```
</details>

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.aggregate([
  { $group: { _id: "$country", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```
</details>

**Q15.** Find the average IMDb rating per director. Use `$group` with `$avg`.

<details>
<summary><b>Hint</b></summary>

```javascript
avg_rating: { $avg: "$ratings.imdb" } 
```
</details>

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.aggregate([
  { $group: { _id: "$director", avg_rating: { $avg: "$ratings.imdb" } } },
  { $sort: { avg_rating: -1 } }
])
```
</details>

**Q16.** Count how many movies exist per genre. Since `genres` is an array, you need
`$unwind` to expand each array element into its own document before grouping.

What does $unwind do?

A document like `{ title: "Inception", genres: ["Action", "Adventure", "Sci-Fi"] }`
becomes three documents:
```
{ title: "Inception", genres: "Action" }
{ title: "Inception", genres: "Adventure" }
{ title: "Inception", genres: "Sci-Fi" }
```

<details>
<summary><b>Answer</b></summary>

```javascript
db.movies.aggregate([
  { $unwind: "$genres" },
  { $group: { _id: "$genres", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```
</details>

---

### Cleanup

When you're done, stop the containers:

```bash
docker compose down
```

To also remove the stored data:

```bash
docker compose down -v
```
