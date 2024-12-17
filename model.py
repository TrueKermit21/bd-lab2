import psycopg2
import time
from sqlalchemy import create_engine, Column, Integer, String, Text, ForeignKey, Date
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()

class Museum(Base):
    __tablename__ = 'museum'

    museum_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    location = Column(String(150), nullable=False)
    established_year = Column(Integer, nullable=True)

    galleries = relationship('Gallery', back_populates='museum')


class Gallery(Base):
    __tablename__ = 'gallery'

    gallery_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    floor = Column(Integer, nullable=True)
    theme = Column(String(100), nullable=True)
    museum_id = Column(Integer, ForeignKey('museum.museum_id'), nullable=False)

    museum = relationship('Museum', back_populates='galleries')
    exhibits = relationship('Exhibit', back_populates='gallery')


class Exhibit(Base):
    __tablename__ = 'exhibit'

    exhibit_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    year_created = Column(Integer, nullable=True)
    gallery_id = Column(Integer, ForeignKey('gallery.gallery_id'), nullable=False)

    gallery = relationship('Gallery', back_populates='exhibits')
    schedules = relationship('ExhibitSchedule', back_populates='exhibit')


class ExhibitSchedule(Base):
    __tablename__ = 'exhibit_schedule'

    exhibit_schedule_id = Column(Integer, primary_key=True, autoincrement=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    exhibit_id = Column(Integer, ForeignKey('exhibit.exhibit_id'), nullable=False)
    gallery_id = Column(Integer, ForeignKey('gallery.gallery_id'), nullable=False)

    exhibit = relationship('Exhibit', back_populates='schedules')
    gallery = relationship('Gallery')


class MuseumGalleryExhibitModel:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='museum-gallery-exhibit-management-system',
            user='postgres',
            password='1234',
            host='localhost',
            port=5432
        )
        
        self.engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/museum-gallery-exhibit-management-system')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_museum(self, name, location, established_year):
        session = self.Session()
        try:
            session.add(Museum(name=name, location=location, established_year=established_year))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding museum: {e}")
        finally:
            session.close()

    def get_museums(self):
        session = self.Session()
        try:
            return [(m.museum_id, m.name, m.location, m.established_year) for m in session.query(Museum).all()]
        finally:
            session.close()

    def delete_museum(self, museum_id):
        session = self.Session()
        try:
            museum = session.get(Museum, museum_id)
            if museum:
                session.delete(museum)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error deleting museum: {e}")
        finally:
            session.close()

    def update_museum(self, museum_id, name, location, established_year):
        session = self.Session()
        try:
            museum = session.get(Museum, museum_id)
            if museum:
                museum.name = name
                museum.location = location
                museum.established_year = established_year
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error updating museum: {e}")
        finally:
            session.close()

    def add_gallery(self, name, floor, theme, museum_id):
        session = self.Session()
        try:
            session.add(Gallery(name=name, floor=floor, theme=theme, museum_id=museum_id))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding gallery: {e}")
        finally:
            session.close()

    def get_galleries(self):
        session = self.Session()
        try:
            return [(g.gallery_id, g.name, g.floor, g.theme, g.museum_id) for g in session.query(Gallery).all()]
        finally:
            session.close()

    def delete_gallery(self, gallery_id):
        session = self.Session()
        try:
            gallery = session.get(Gallery, gallery_id)
            if gallery:
                session.delete(gallery)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error deleting gallery: {e}")
        finally:
            session.close()

    def update_gallery(self, gallery_id, name, floor, theme, museum_id):
        session = self.Session()
        try:
            gallery = session.get(Gallery, gallery_id)
            if gallery:
                gallery.name = name
                gallery.floor = floor
                gallery.theme = theme
                gallery.museum_id = museum_id
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error updating gallery: {e}")
        finally:
            session.close()

    def add_exhibit(self, name, description, year_created, gallery_id):
        session = self.Session()
        try:
            session.add(Exhibit(name=name, description=description, year_created=year_created, gallery_id=gallery_id))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding exhibit: {e}")
        finally:
            session.close()

    def get_exhibits(self):
        session = self.Session()
        try:
            return [(e.exhibit_id, e.name, e.description, e.year_created, e.gallery_id) for e in session.query(Exhibit).all()]
        finally:
            session.close()

    def delete_exhibit(self, exhibit_id):
        session = self.Session()
        try:
            exhibit = session.get(Exhibit, exhibit_id)
            if exhibit:
                session.delete(exhibit)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error deleting exhibit: {e}")
        finally:
            session.close()

    def update_exhibit(self, exhibit_id, name, description, year_created, gallery_id):
        session = self.Session()
        try:
            exhibit = session.get(Exhibit, exhibit_id)
            if exhibit:
                exhibit.name = name
                exhibit.description = description
                exhibit.year_created = year_created
                exhibit.gallery_id = gallery_id
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error updating exhibit: {e}")
        finally:
            session.close()

    def add_exhibit_schedule(self, start_date, end_date, exhibit_id, gallery_id):
        session = self.Session()
        try:
            session.add(ExhibitSchedule(start_date=start_date, end_date=end_date, exhibit_id=exhibit_id, gallery_id=gallery_id))
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error adding exhibit schedule: {e}")
        finally:
            session.close()

    def get_exhibit_schedule(self):
        session = self.Session()
        try:
            return [(s.exhibit_schedule_id, s.start_date, s.end_date, s.exhibit_id, s.gallery_id) for s in session.query(ExhibitSchedule).all()]
        finally:
            session.close()

    def delete_exhibit_schedule(self, schedule_id):
        session = self.Session()
        try:
            schedule = session.get(ExhibitSchedule, schedule_id)
            if schedule:
                session.delete(schedule)
                session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error deleting exhibit schedule: {e}")
        finally:
            session.close()

    def generate_random_data(self, iterations=5):
        c = self.conn.cursor()

        for _ in range(iterations):
            # Insert one random row into Museum
            c.execute('''
                INSERT INTO Museum (Name, Location, Established_Year)
                VALUES (
                    'Museum ' || trunc(random() * 1000 + 1)::int,
                    'Location ' || trunc(random() * 100 + 1)::int,
                    (1900 + trunc(random() * 123))::int
                );
            ''')

            # Insert one random row into Gallery with valid Museum_ID
            c.execute('''
                INSERT INTO Gallery (Name, Floor, Theme, Museum_ID)
                VALUES (
                    'Gallery ' || trunc(random() * 1000 + 1)::int,
                    trunc(random() * 10 + 1)::int,
                    'Theme ' || trunc(random() * 100 + 1)::int,
                    (SELECT Museum_ID FROM Museum OFFSET floor(random() * (SELECT COUNT(*) FROM Museum)) LIMIT 1)
                );
            ''')

            # Insert one random row into Exhibit with valid Gallery_ID
            c.execute('''
                INSERT INTO Exhibit (Name, Description, Year_Created, Gallery_ID)
                VALUES (
                    'Exhibit ' || trunc(random() * 1000 + 1)::int,
                    'Description ' || trunc(random() * 1000 + 1)::int,
                    (1800 + trunc(random() * 223))::int,
                    (SELECT Gallery_ID FROM Gallery OFFSET floor(random() * (SELECT COUNT(*) FROM Gallery)) LIMIT 1)
                );
            ''')

            # Insert one random row into Exhibit_Schedule with valid Exhibit_ID and Gallery_ID
            c.execute('''
                INSERT INTO Exhibit_Schedule (Start_Date, End_Date, Exhibit_ID, Gallery_ID)
                VALUES (
                    current_date + (random() * 365)::int,
                    current_date + (random() * 365 + 365)::int,
                    (SELECT Exhibit_ID FROM Exhibit OFFSET floor(random() * (SELECT COUNT(*) FROM Exhibit)) LIMIT 1),
                    (SELECT Gallery_ID FROM Gallery OFFSET floor(random() * (SELECT COUNT(*) FROM Gallery)) LIMIT 1)
                );
            ''')

        self.conn.commit()

    def query_museum_gallery(self, location, floor):
        c = self.conn.cursor()
        start_time = time.time()
        c.execute('''
            SELECT m.Name AS Museum_Name, g.Name AS Gallery_Name, g.Floor
            FROM Museum m
            INNER JOIN Gallery g ON m.Museum_ID = g.Museum_ID
            WHERE m.Location = %s AND g.Floor = %s
            GROUP BY m.Name, g.Name, g.Floor
        ''', (location, floor))
        rows = c.fetchall()
        execution_time = (time.time() - start_time) * 1000
        return rows, execution_time

    def query_exhibit_schedule(self, start_date, end_date):
        c = self.conn.cursor()
        start_time = time.time()
        c.execute('''
            SELECT e.Name AS Exhibit_Name, s.Start_Date, s.End_Date
            FROM Exhibit e
            INNER JOIN Exhibit_Schedule s ON e.Exhibit_ID = s.Exhibit_ID
            WHERE s.Start_Date >= %s AND s.End_Date <= %s
            GROUP BY e.Name, s.Start_Date, s.End_Date
        ''', (start_date, end_date))
        rows = c.fetchall()
        execution_time = (time.time() - start_time) * 1000
        return rows, execution_time

    def query_museum_exhibit_count(self, established_year):
        c = self.conn.cursor()
        start_time = time.time()
        c.execute('''
            SELECT m.Name AS Museum_Name, COUNT(e.Exhibit_ID) AS Exhibit_Count
            FROM Museum m
            INNER JOIN Gallery g ON m.Museum_ID = g.Museum_ID
            INNER JOIN Exhibit e ON g.Gallery_ID = e.Gallery_ID
            WHERE m.Established_Year >= %s
            GROUP BY m.Name
        ''', (established_year,))
        rows = c.fetchall()
        execution_time = (time.time() - start_time) * 1000
        return rows, execution_time