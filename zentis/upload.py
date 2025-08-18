import logging
from nemo_library import NemoLibrary
from nemo_library.model.project import Project

from symbols import PROJECT_NAME_FERTIGARTIKEL
from symbols import PROJECT_NAME_REZEPTURDATEN
from symbols import DATAPATH


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

nl = NemoLibrary()

# delete all zentis projects
logging.info("Deleting existing Zentis projects...")
projects = nl.getProjects()
projectids = [
    project.id for project in projects if project.displayName.startswith("Zentis")
]
nl.deleteProjects(projectids)

# create projects and upload meta data
logging.info("Creating Zentis projects...")
projects: list[Project] = []
projects.append(
    Project(
        displayName=PROJECT_NAME_FERTIGARTIKEL,
        description="This is a project for Zentis.",
    )
)
projects.append(
    Project(
        displayName=PROJECT_NAME_REZEPTURDATEN,
        description="This is a project for Zentis.",
    )
)
nl.createProjects(projects=projects)

# set meta data
nl = NemoLibrary(metadata_directory="./metadata/Fertigartikel")
nl.MetaDataCreate(projectname=PROJECT_NAME_FERTIGARTIKEL)

nl = NemoLibrary(metadata_directory="./metadata/Rezepturdaten")
nl.MetaDataCreate(projectname=PROJECT_NAME_REZEPTURDATEN)

nl = NemoLibrary()

# upload files
logging.info("Uploading files to Zentis projects...")
nl.ReUploadFile(
    projectname=PROJECT_NAME_FERTIGARTIKEL,
    filename=DATAPATH / "V_NemoAI_Fertigartikel_IST_PLAN.csv",
    update_project_settings=False,
)
nl.ReUploadFile(
    projectname=PROJECT_NAME_REZEPTURDATEN,
    filename=DATAPATH / "V_NemoAI_Rezepturdaten.csv",
    update_project_settings=False,
)
