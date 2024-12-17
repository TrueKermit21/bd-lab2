from model import MuseumGalleryExhibitModel
from view import MuseumGalleryExhibitView


class MuseumGalleryExhibitController:
    def __init__(self):
        self.model = MuseumGalleryExhibitModel()
        self.view = MuseumGalleryExhibitView()

    def run(self):
        while True:
            choice = self.show_menu()
            if choice == '0':
                break
            elif choice == '1':
                self.add_museum()
            elif choice == '2':
                self.view_museums()
            elif choice == '3':
                self.update_museum()
            elif choice == '4':
                self.delete_museum()
            elif choice == '5':
                self.add_gallery()
            elif choice == '6':
                self.view_galleries()
            elif choice == '7':
                self.update_gallery()
            elif choice == '8':
                self.delete_gallery()
            elif choice == '9':
                self.add_exhibit()
            elif choice == '10':
                self.view_exhibits()
            elif choice == '11':
                self.update_exhibit()
            elif choice == '12':
                self.delete_exhibit()
            elif choice == '13':
                self.add_exhibit_schedule()
            elif choice == '14':
                self.view_exhibit_schedule()
            elif choice == '15':
                self.delete_exhibit_schedule()
            elif choice == '16':
                self.generate_random_data()
            elif choice == '17':
                self.query_museum_gallery()
            elif choice == '18':
                self.query_exhibit_schedule()
            elif choice == '19':
                self.query_museum_exhibit_count()

    def show_menu(self):
        self.view.show_message("\nMenu:")
        self.view.show_message("1. Add Museum")
        self.view.show_message("2. View Museums")
        self.view.show_message("3. Update Museum")
        self.view.show_message("4. Delete Museum")
        self.view.show_message("5. Add Gallery")
        self.view.show_message("6. View Galleries")
        self.view.show_message("7. Update Gallery")
        self.view.show_message("8. Delete Gallery")
        self.view.show_message("9. Add Exhibit")
        self.view.show_message("10. View Exhibits")
        self.view.show_message("11. Update Exhibit")
        self.view.show_message("12. Delete Exhibit")
        self.view.show_message("13. Add Exhibit Schedule")
        self.view.show_message("14. View Exhibit Schedules")
        self.view.show_message("15. Delete Exhibit Schedule")
        self.view.show_message("16. Generate Random Data")
        self.view.show_message("17. Query Museum-Gallery")
        self.view.show_message("18. Query Exhibit Schedule")
        self.view.show_message("19. Query Museum Exhibit Count")
        self.view.show_message("0. Exit")
        return input("Enter your choice: ")

    def add_museum(self):
        name, location, established_year = self.view.get_museum_input()
        self.model.add_museum(name, location, established_year)

    def view_museums(self):
        museums = self.model.get_museums()
        self.view.show_museums(museums)

    def update_museum(self):
        museum_id = self.view.get_id("museum")
        name, location, established_year = self.view.get_museum_input()
        self.model.update_museum(museum_id, name, location, established_year)

    def delete_museum(self):
        museum_id = self.view.get_id("museum")
        self.model.delete_museum(museum_id)

    def add_gallery(self):
        name, floor, theme, museum_id = self.view.get_gallery_input()
        self.model.add_gallery(name, floor, theme, museum_id)

    def view_galleries(self):
        galleries = self.model.get_galleries()
        self.view.show_galleries(galleries)

    def update_gallery(self):
        gallery_id = self.view.get_id("gallery")
        name, floor, theme, museum_id = self.view.get_gallery_input()
        self.model.update_gallery(gallery_id, name, floor, theme, museum_id)

    def delete_gallery(self):
        gallery_id = self.view.get_id("gallery")
        self.model.delete_gallery(gallery_id)

    def add_exhibit(self):
        name, description, year_created, gallery_id = self.view.get_exhibit_input()
        self.model.add_exhibit(name, description, year_created, gallery_id)

    def view_exhibits(self):
        exhibits = self.model.get_exhibits()
        self.view.show_exhibits(exhibits)

    def update_exhibit(self):
        exhibit_id = self.view.get_id("exhibit")
        name, description, year_created, gallery_id = self.view.get_exhibit_input()
        self.model.update_exhibit(exhibit_id, name, description, year_created, gallery_id)

    def delete_exhibit(self):
        exhibit_id = self.view.get_id("exhibit")
        self.model.delete_exhibit(exhibit_id)

    def add_exhibit_schedule(self):
        start_date, end_date, exhibit_id, gallery_id = self.view.get_exhibit_schedule_input()
        self.model.add_exhibit_schedule(start_date, end_date, exhibit_id, gallery_id)

    def view_exhibit_schedule(self):
        schedules = self.model.get_exhibit_schedule()
        self.view.show_exhibit_schedule(schedules)

    def delete_exhibit_schedule(self):
        schedule_id = self.view.get_id("exhibit schedule")
        self.model.delete_exhibit_schedule(schedule_id)

    def generate_random_data(self):
        self.model.generate_random_data()
        self.view.show_message("Random data generated successfully!")

    def query_museum_gallery(self):
        location = input("Enter museum location for filtering: ")
        floor = int(input("Enter gallery floor for filtering: "))
        rows, exec_time = self.model.query_museum_gallery(location, floor)
        self.view.show_query_results(rows)
        self.view.show_message(f"Query executed in {exec_time:.2f} ms.")

    def query_exhibit_schedule(self):
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        rows, exec_time = self.model.query_exhibit_schedule(start_date, end_date)
        self.view.show_query_results(rows)
        self.view.show_message(f"Query executed in {exec_time:.2f} ms.")

    def query_museum_exhibit_count(self):
        established_year = int(input("Enter established year to filter museums: "))
        rows, exec_time = self.model.query_museum_exhibit_count(established_year)
        self.view.show_query_results(rows)
        self.view.show_message(f"Query executed in {exec_time:.2f} ms.")