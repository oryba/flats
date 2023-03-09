import json
from datetime import datetime

import requests
from sqlalchemy import Column, select
from sqlalchemy import Integer
from sqlalchemy import Text, Boolean, Numeric, DateTime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import declarative_base

rates = {
    'UAH': 39.1,
    'USD': 1,
    'EUR': 39.8
}

Base = declarative_base()


class Selection(Base):
    __tablename__ = 'selection'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    query = Column(Text)

    def __repr__(self):
        return f"Selection (title={self.title})"


class Offer(Base):
    __tablename__ = 'offer'

    record_id = Column(Integer, primary_key=True)
    flat_id = Column(Integer)
    selection_id = Column(Integer, foreign_key=Selection.id)
    area = Column(Numeric)
    price = Column(Integer)
    floor = Column(Integer)
    scan_date = Column(DateTime)
    insert_date = Column(DateTime)
    renovation = Column(Boolean)

    def __repr__(self):
        return f"Offer(building={self.building_id})"


engine = create_engine("sqlite:///data/flats.db", echo=True, future=True, connect_args={'check_same_thread': False})
conn = engine.connect()
Base.metadata.create_all(engine)

session = Session(engine)


def fetch_fav(selection: Selection):
    payload = '&'.join([f'id={el["realty_id"]}' for el in json.loads(selection.query)])
    url = "https://flatfy.ua/api/realties/batch"
    response = requests.request("GET", url + "?" + payload)
    return response.json()['data']


def fetch_query(selection: Selection):
    data = []
    page = 1

    while True:
        url = f"https://flatfy.ua/api/realties?page={page}&currency=USD&{selection.query}&price_min=20000&section_id=1&lang=uk"

        len_before = len(data)
        response = requests.request("GET", url + "&with_renovation=yes")
        data.extend([{**item, 'renovation': True} for item in response.json()['data']])

        response = requests.request("GET", url + "&with_renovation=no")
        data.extend(response.json()['data'])
        if len(data) == len_before:
            break
        page += 1
    return data


def fetch_data(selections=None):
    scan_date = datetime.now()

    updated_flats = []

    for selection in session.scalars(select(Selection)):
        if selections and selection.id not in selections:
            continue

        if selection.id == 0:
            data = fetch_fav(selection)
        else:
            data = fetch_query(selection)

        for d in data:
            d['price'] /= rates[d['currency']]

        offers = []
        for flat in data:
            offers.append(Offer(
                selection_id=selection.id,
                flat_id=flat['id'],
                area=flat['area_total'],
                price=flat['price'],
                floor=flat['floor'],
                insert_date=datetime.fromisoformat(flat['insert_time']),
                scan_date=scan_date,
                renovation=flat.get('renovation', False)
            ))
        session.bulk_save_objects(offers)
        updated_flats.extend([o.flat_id for o in offers])

    session.query(Offer).filter(
        Offer.flat_id.in_(updated_flats),
        Offer.scan_date >= scan_date.replace(hour=0, minute=0, second=0, microsecond=0),
        Offer.scan_date != scan_date
    ).delete()
    session.commit()


def fetch_insights():
    pass


if __name__ == "__main__":
    fetch_data()
