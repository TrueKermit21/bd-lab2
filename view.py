class MuseumGalleryExhibitView:
    def show_museums(self, museums):
        print("Museums:")
        for museum in museums:
            print(f"ID: {museum[0]}, Name: {museum[1]}, Location: {museum[2]}, Established Year: {museum[3]}")

    def show_galleries(self, galleries):
        print("Galleries:")
        for gallery in galleries:
            print(f"ID: {gallery[0]}, Name: {gallery[1]}, Floor: {gallery[2]}, Theme: {gallery[3]}, Museum ID: {gallery[4]}")

    def show_exhibits(self, exhibits):
        print("Exhibits:")
        for exhibit in exhibits:
            print(f"ID: {exhibit[0]}, Name: {exhibit[1]}, Description: {exhibit[2]}, Year Created: {exhibit[3]}, Gallery ID: {exhibit[4]}")

    def show_exhibit_schedule(self, schedules):
        print("Exhibit Schedules:")
        for schedule in schedules:
            print(f"ID: {schedule[0]}, Start Date: {schedule[1]}, End Date: {schedule[2]}, Exhibit ID: {schedule[3]}, Gallery ID: {schedule[4]}")

    def get_museum_input(self):
        name = input("Enter museum name: ")
        location = input("Enter museum location: ")
        established_year = input("Enter museum established year (optional): ")
        established_year = int(established_year) if established_year else None
        return name, location, established_year

    def get_gallery_input(self):
        name = input("Enter gallery name: ")
        floor = int(input("Enter gallery floor: "))
        theme = input("Enter gallery theme (optional): ")
        museum_id = int(input("Enter associated museum ID: "))
        return name, floor, theme, museum_id

    def get_exhibit_input(self):
        name = input("Enter exhibit name: ")
        description = input("Enter exhibit description: ")
        year_created = int(input("Enter year created: "))
        gallery_id = int(input("Enter associated gallery ID: "))
        return name, description, year_created, gallery_id

    def get_exhibit_schedule_input(self):
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        exhibit_id = int(input("Enter exhibit ID: "))
        gallery_id = int(input("Enter gallery ID: "))
        return start_date, end_date, exhibit_id, gallery_id

    def get_id(self, entity_name):
        return int(input(f"Enter {entity_name} ID: "))

    def show_message(self, message):
        print(message)

    def show_query_results(self, rows):
        print("Query Results:")
        for row in rows:
            print(row)
