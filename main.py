import sqlalchemy
import configparser
from sqlalchemy.orm import sessionmaker
from orm import create_tables, Publisher, Shop, Book, Stock, Sale
from data_parse_tools import parse_json

if __name__ == '__main__':
    # загрузка конфига
    config = configparser.ConfigParser()
    config.read("settings.ini")
    db = config["database"]["db"]
    login = config["credentials"]["login"]
    password = config["credentials"]["password"]

    # создание подключения
    DSN = f"postgresql://{login}:{password}@localhost:5432/{db}"
    engine = sqlalchemy.create_engine(DSN)

    # создание таблиц
    create_tables(engine)

    # создание сессии
    Session = sessionmaker(bind=engine)
    session = Session()

    # считывание файла
    data = parse_json()

    # создание объектов
    for row in data:
        if row['model'] == 'publisher':
            publisher = Publisher(id=row['pk'], name=row['fields']['name'])
            session.add(publisher)
        elif row['model'] == 'book':
            book = Book(id=row['pk'], title=row['fields']['title'], id_publisher=row['fields']['id_publisher'])
            session.add(book)
        elif row['model'] == 'shop':
            shop = Shop(id=row['pk'], name=row['fields']['name'])
            session.add(shop)
        elif row['model'] == 'stock':
            stock = Stock(
                id=row['pk'],
                id_shop=row['fields']['id_shop'],
                id_book=row['fields']['id_book'],
                count=row['fields']['count']
            )
            session.add(stock)
        elif row['model'] == 'sale':
            stock = Sale(
                id=row['pk'],
                price=row['fields']['price'],
                date_sale=row['fields']['date_sale'],
                count=row['fields']['count'],
                id_stock=row['fields']['id_stock']
            )
            session.add(stock)
    session.commit()

    # поиск магазинов по издателю
    publisher_id = input('Id издателя: ')
    books = session.query(Book).join(Publisher.book).filter(Publisher.id == publisher_id).subquery()
    shops = session.query(Stock).join(books, Stock.id_book == books.c.id).subquery()
    q = session.query(Shop).join(shops, Shop.id == shops.c.id_shop)
    for s in q.all():
        print("Книги искомого издателя в наличии в магазине", s.name)
