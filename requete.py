from pymongo import MongoClient

# Connexion à MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBLP'] 
collection = db['publis'] 

print("\nRequête 1 : Afficher toutes les publications de type livre (Book)")
books = collection.find({ "type": "Book" }).limit(3)
for book in books:
    print(book)

print("\nRequête 2 : Afficher la liste des publications depuis 2012")
publications_since_2012 = collection.find({ "year": { "$gte": 2012 } }).limit(3)
for publication in publications_since_2012:
    print(publication)

print("\nRequête 3 : Afficher la liste des publications de type livre depuis 2012")
books_since_2012 = collection.find({ "type": "Book", "year": { "$gte": 2012 } }).limit(3)
for book in books_since_2012:
    print(book)

print("\nRequête 4 : Afficher la liste des publications de l'auteur 'Michael Schmitz'")
publications_by_author = collection.find({ "authors": "Michael Schmitz" }).limit(3)
for pub in publications_by_author:
    print(pub)

print("\nRequête 5 : Donner la liste de tous les éditeurs (publisher) distincts")
distinct_publishers = collection.aggregate([
    { "$group": { "_id": "$publisher" } },
    { "$limit": 3 }
])
for publisher in distinct_publishers:
    print(publisher["_id"])

print("\nRequête 6 : Donner la liste de tous les auteurs (authors) distincts")
distinct_authors = collection.aggregate([
    { "$group": { "_id": "$authors" } },
    { "$limit": 3 }
])
for author in distinct_authors:
    print(author["_id"])

print("\nRequête 7 : Trier les publications de l'auteur 'Toru Ishida' par titre de livre et par page de début")
publications_toru_ishida = collection.find({ "authors": "Toru Ishida" }).sort([("title", 1), ("pages.start", 1)]).limit(3)
for pub in publications_toru_ishida:
    print(pub)

print("\nRequête 8 : Projeter le résultat sur le titre de la publication et les pages")
publications_toru_ishida_proj = collection.find(
    { "authors": "Toru Ishida" },
    { "title": 1, "pages": 1, "_id": 0 }
).limit(3)
for pub in publications_toru_ishida_proj:
    print(pub)

count_toru_ishida = collection.count_documents({ "authors": "Toru Ishida" })
print("\nRequête 9 : Nombre de publications de Toru Ishida:")
print(f"Nombre de publications de Toru Ishida: {count_toru_ishida}")

print("\nRequête 10 : Donner le nombre de publications depuis 2011 et par type")
publications_by_type = collection.aggregate([
    { "$match": { "year": { "$gte": 2011 } } },
    { "$group": { "_id": "$type", "total": { "$sum": 1 } } },
    { "$limit": 3 }
])
for pub in publications_by_type:
    print(f"Type: {pub['_id']}, Total: {pub['total']}")

print("\nRequête 11 : Donner le nombre de publications par auteur et trier le résultat par ordre croissant")
publications_by_author = collection.aggregate([
    { "$unwind": "$authors" },
    { "$group": { "_id": "$authors", "total": { "$sum": 1 } } },
    { "$sort": { "total": 1 } },
    { "$limit": 3 }
])
for pub in publications_by_author:
    print(f"Auteur: {pub['_id']}, Total: {pub['total']}")
