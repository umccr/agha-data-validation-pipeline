import re
from enum import Enum
from typing import List

AGHA_ID_PATTERN = re.compile("A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?|unknown")
MD5_PATTERN = re.compile("[0-9a-f]{32}")


class FlagShip(Enum):
    """
    Official AGHA flagship projects.
    Tuple values are (in order):
    - preferred flagship code
    - official flagship name
    - aliases
    """
    ACUTE_CARE_GENOMICS = ('AC', 'Acute Care Genomics', 'ACG', 'acute', 'acutecare', 'acute_care_genomics')
    MITOCHONDRIAL_DISORDERS = ('Mito', 'Mitochondrial Disorders', 'mito', 'MITO', 'mito-batch-7',
                               'mitochondrial_disease')
    EPILEPTIC_ENCEPHALOPATHY = ('EE', 'Epileptic Encephalopathy')
    GENETIC_IMMUNOLOGY = ('GI', 'Genetic Immunology')
    LEUKODYSTROPHIES = ('Leuko', 'Leukodystrophies', 'LD', 'leukodystrophies')
    BRAIN_MALFORMATIONS = ('BM', 'Brain Malformations', 'brain_malformations', 'MCD')
    CHILDRANZ = ('chILDRANZ', 'Interstitial and Diffuse Lung Disease in Children', 'CHW')
    RENAL_GENETICS = ('KidGen', 'Renal Genetics', 'RENAL', 'renal')
    NEUROMUSCULAR_DISORDERS = ('NMD', 'Neuromuscular Disorders')
    HEREDITARY_CANCERS = ('ICCon', 'Hereditary Cancers', 'ICCON')
    INTELLECTUAL_DISABILITY = ('ID', 'Intellectual Disability', 'id')
    CARDIOVASCULAR_GENETIC_DISORDERS = ('Cardiac', 'Cardiovascular Genetic Disorders', 'CARDIAC', 'cardiac')
    HIDDEN_RENAL_GENETICS = ('HIDDEN', 'HIDDEN Renal Genetics', 'hidden')
    TEST = ('TEST', 'Test submissions')
    UNKNOWN = ('UNKNOWN', 'Unknown flagship')

    def preferred_code(self) -> str:
        return self.value[0]

    def official_name(self) -> str:
        return self.value[1]

    def aliases(self) -> List[str]:
        return self.value[2:]

    @staticmethod
    def from_name(name: str) -> 'FlagShip':
        for fs in FlagShip:
            if name in fs.value:
                return fs
        return FlagShip.UNKNOWN


class FileType(Enum):
    BAM = "BAM"
    BAM_INDEX = "BAM_INDEX"
    CRAM = "CRAM"
    CRAM_INDEX = "CRAM_INDEX"
    FASTQ = "FASTQ"
    VCF = "VCF"
    VCF_INDEX = "VCF_INDEX"
    MD5 = "MD5"
    MANIFEST = "MANIFEST"
    OTHER = "OTHER"

    def __str__(self):
        return self.value


FEXT_FASTQ = ['.fq', '.fq.gz', '.fastq', '.fastq.gz']
FEXT_BAM = ['.bam']
FEXT_BAM_INDEX = ['.bai']
FEXT_VCF = ['vcf.gz', 'vcf']
FEXT_VCF_INDEX = ['.tbi']
FEXT_MANIFEST = ['manifest.txt', '.manifest']


class FileType(Enum):
    UNSUPPORTED = ('UNSUPPORTED', [])
    FASTQ = ('FASTQ', FEXT_FASTQ)
    VCF = ('VCF', FEXT_VCF)
    VCF_INDEX = ('VCF_INDEX', FEXT_VCF_INDEX)
    BAM = ('BAM', FEXT_BAM)
    BAM_INDEX = ('BAM_INDEX', FEXT_BAM_INDEX)
    MANIFEST = ('MANIFEST', FEXT_MANIFEST)

    def get_name(self) -> str:
        return self.value[0]

    def get_extensions(self) -> List[str]:
        return self.value[1]

    def is_type(self, name: str) -> bool:
        return any(name.endswith(ext) for ext in self.get_extensions())

    @staticmethod
    def from_name(name: str) -> 'FileType':
        for t in FileType:
            if t.is_type(name):
                return t
        return FileType.UNSUPPORTED
