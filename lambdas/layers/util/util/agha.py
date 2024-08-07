import re
from enum import Enum
from typing import List

AGHA_ID_PATTERN = re.compile(r"A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?|unknown")
MD5_PATTERN = re.compile("[0-9a-f]{32}")


class FlagShip(Enum):
    """
    Official AGHA flagship projects.
    Tuple values are (in order):
    - preferred flagship code
    - official flagship name
    - aliases
    """

    ACUTE_CARE_GENOMICS = (
        "AC",
        "Acute Care Genomics",
        "ACG",
        "acute",
        "acutecare",
        "acute_care_genomics",
    )
    MITOCHONDRIAL_DISORDERS = (
        "Mito",
        "Mitochondrial Disorders",
        "mito",
        "MITO",
        "mito-batch-7",
        "mitochondrial_disease",
    )
    EPILEPTIC_ENCEPHALOPATHY = ("EE", "Epileptic Encephalopathy")
    GENETIC_IMMUNOLOGY = ("GI", "Genetic Immunology")
    LEUKODYSTROPHIES = ("Leuko", "Leukodystrophies", "LD", "leukodystrophies")
    BRAIN_MALFORMATIONS = ("BM", "Brain Malformations", "brain_malformations", "MCD")
    CHILDRANZ = (
        "chILDRANZ",
        "Interstitial and Diffuse Lung Disease in Children",
        "CHW",
    )
    RENAL_GENETICS = ("KidGen", "Renal Genetics", "RENAL", "renal")
    NEUROMUSCULAR_DISORDERS = ("NMD", "Neuromuscular Disorders")
    HEREDITARY_CANCERS = ("ICCon", "Hereditary Cancers", "ICCON")
    INTELLECTUAL_DISABILITY = ("ID", "Intellectual Disability", "id")
    CARDIOVASCULAR_GENETIC_DISORDERS = (
        "Cardiac",
        "Cardiovascular Genetic Disorders",
        "CARDIAC",
        "cardiac",
    )
    HIDDEN_RENAL_GENETICS = ("HIDDEN", "HIDDEN Renal Genetics", "hidden")

    # Mackenzie's Mission
    MM_VCGS = ("MM_VCGS", "MM_VCGS", "MM_VCGS")
    MM_NSWHP = ("MM_NSWHP", "MM_NSWHP", "MM_NSWHP")
    MM_PATHWEST = ("MM_PATHWEST", "MM_PATHWEST", "MM_PATHWEST")

    TEST = ("TEST", "Test submissions")
    UNKNOWN = ("UNKNOWN", "Unknown flagship")

    def preferred_code(self) -> str:
        return self.value[0]

    def official_name(self) -> str:
        return self.value[1]

    def aliases(self) -> List[str]:
        return self.value[2:]

    @staticmethod
    def from_name(name: str) -> "FlagShip":
        for fs in FlagShip:
            if name in fs.value:
                return fs
        return FlagShip.UNKNOWN

    @staticmethod
    def list_flagship_enum() -> List:
        return [flagship.preferred_code() for flagship in FlagShip]


FEXT_FASTQ = [".fq", ".fq.gz", ".fastq", ".fastq.gz"]
FEXT_BAM = [".bam"]
FEXT_BAM_INDEX = [".bai"]
FEXT_CRAM = [".cram"]
FEXT_CRAM_INDEX = [".crai"]
FEXT_VCF = ["vcf.gz", "vcf"]
FEXT_VCF_INDEX = [".tbi"]
FEXT_MANIFEST = ["manifest.txt", ".manifest", "manifest.orig"]


class FileType(Enum):
    UNSUPPORTED = ("UNSUPPORTED", [])
    FASTQ = ("FASTQ", FEXT_FASTQ)
    VCF = ("VCF", FEXT_VCF)
    VCF_INDEX = ("VCF_INDEX", FEXT_VCF_INDEX)
    BAM = ("BAM", FEXT_BAM)
    BAM_INDEX = ("BAM_INDEX", FEXT_BAM_INDEX)
    CRAM = ("CRAM", FEXT_CRAM)
    CRAM_INDEX = ("CRAM_INDEX", FEXT_CRAM_INDEX)
    MANIFEST = ("MANIFEST", FEXT_MANIFEST)

    def get_name(self) -> str:
        return self.value[0]

    def get_extensions(self) -> List[str]:
        return self.value[1]

    def is_type(self, name: str) -> bool:
        return any(name.endswith(ext) for ext in self.get_extensions())

    @staticmethod
    def from_name(name: str) -> "FileType":
        for t in FileType:
            if t.is_type(name):
                return t
        return FileType.UNSUPPORTED

    @staticmethod
    def from_enum_name(name: str) -> "FileType":
        for t in FileType:
            if t.get_name() == name:
                return t
        return FileType.UNSUPPORTED

    @staticmethod
    def is_indexable(filetype: str) -> bool:
        INDEXABLE_TYPE = [
            FileType.BAM.get_name(),
            FileType.CRAM.get_name(),
            FileType.VCF.get_name(),
        ]
        if filetype in INDEXABLE_TYPE:
            return True
        return False

    @staticmethod
    def is_compressable(filetype: str) -> bool:
        COMPRESSABLE_TYPE = [FileType.VCF.get_name(), FileType.FASTQ.get_name()]
        if filetype in COMPRESSABLE_TYPE:
            return True
        return False

    @staticmethod
    def is_compressable_file(filename: str) -> bool:
        COMPRESSABLE_TYPE = [FileType.VCF, FileType.FASTQ]
        ft = FileType.from_name(filename)
        if ft in COMPRESSABLE_TYPE:
            return True
        return False

    @staticmethod
    def is_index_file(filename: str) -> bool:
        INDEX_FILE = [FileType.BAM_INDEX, FileType.CRAM_INDEX, FileType.VCF_INDEX]
        if FileType.from_name(filename) in INDEX_FILE:
            return True
        return False

    @staticmethod
    def is_compress_file(filename: str) -> bool:
        if filename.endswith(".gz"):
            return True
        return False

    @staticmethod
    def is_manifest_file(filename: str) -> bool:
        if FileType.from_name(filename) == FileType.MANIFEST:
            return True
        return False

    @staticmethod
    def is_md5_file(filename: str) -> bool:
        if filename.endswith(".md5"):
            return True
        else:
            return False
