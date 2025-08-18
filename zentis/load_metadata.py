import logging
from nemo_library import NemoLibrary

from symbols import PROJECT_NAME_FERTIGARTIKEL, PROJECT_NAME_REZEPTURDATEN

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

nl = NemoLibrary(metadata_directory="./metadata/Fertigartikel")
nl.MetaDataLoad(projectname=PROJECT_NAME_FERTIGARTIKEL)

nl = NemoLibrary(metadata_directory="./metadata/Rezepturdaten")
nl.MetaDataLoad(projectname=PROJECT_NAME_REZEPTURDATEN)