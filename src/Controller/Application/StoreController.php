<?php

namespace App\Controller\Application;

use App\Attributes\ImportExportAttribute;
use App\Attributes\ImportProcessorAttribute;
use App\Constants\PhpSpreadsheetConstants;
use App\Entity\Brand;
use App\Entity\Store;
use App\Enums\StoreStatus;
use App\Services\ExportService;
use App\Services\FileUploadService;
use App\Services\ImportService;
use App\Services\StoreService;
use App\ViewModels\StoreViewModel;
use App\ViewModels\UserViewModel;
use Doctrine\ORM\EntityManagerInterface;
use Doctrine\ORM\NonUniqueResultException;
use Doctrine\ORM\NoResultException;
use Doctrine\ORM\QueryBuilder;
use PhpOffice\PhpSpreadsheet\Exception;
use ReflectionClass;
use Symfony\Component\DependencyInjection\ParameterBag\ParameterBagInterface;
use Symfony\Component\HttpFoundation\File\UploadedFile;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpFoundation\Session\SessionInterface;
use Symfony\Component\HttpFoundation\StreamedResponse;
use Symfony\Component\Routing\Annotation\Route;
use Symfony\Component\Routing\RouterInterface;
use Symfony\Component\Security\Core\Security;
use Symfony\Component\Serializer\SerializerInterface;
use Symfony\Component\Validator\Constraints;
use Symfony\Component\Validator\ConstraintViolation;
use Symfony\Component\Validator\Validator\ValidatorInterface;

class StoreController extends BaseVueController
{
    protected ?string $entity = StoreViewModel::class;

    public function __construct(
        private readonly SerializerInterface $serializer,
        private readonly Security $security,
        private readonly ParameterBagInterface $parameterBag,
        private readonly SessionInterface $session,
        protected EntityManagerInterface $entityManager,
        protected RouterInterface $router,
        private readonly StoreService $storeService,
        private readonly ImportService $importService,
    ) {
        parent::__construct(
            $serializer,
            $security,
            $parameterBag,
            $session,
            $entityManager,
            $router
        );
    }

    #[Route('/api/stores/index', name: 'api_stores', methods: ['POST', 'GET'])]
    public function index(Request $request): JsonResponse
    {
        return $this->_index($request);
    }

    /**
     * @throws \Exception
     */
    protected function getBaseResults(Request $request): array
    {
        return $this->_getBaseResults($request);
    }

    #[Route('/api/stores/filters', name: 'api_stores_filters', methods: ['POST', 'GET'])]
    public function getFilters(): JsonResponse
    {
        return $this->_getFilters();
    }

    #[Route('/api/filters/brands', name: 'api_brands_filter')]
    public function getBrandsFilter(): JsonResponse
    {
        $brands = $this->entityManager->query('select id, name from brand order by name');
        return $this->json($brands);
    }

    #[Route('/api/stores/temporary-folder', name: 'api_stores_temporary_folder', methods: 'GET')]
    public function getTemporaryUploadFolder(): JsonResponse
    {
        return $this->json(['folder' => sp_unique_string_based_on_uniqid('store')]);
    }

    #[Route('/api/stores/import/upload', name: 'api_stores_upload', methods: 'POST')]
    public function storeImportFileUpload(Request $request, FileUploadService $fileUploadService): JsonResponse
    {
        $files = $request->files;
        $temporaryFolder = $request->headers->get('X-Folder');
        if (!$files) {
            return $this->json([], Response::HTTP_PRECONDITION_FAILED);
        }

        $mapFiles = static function (UploadedFile $item) use ($temporaryFolder, $fileUploadService) {
            return [
                'size' => $item->getSize(),
                'type' => $item->getClientMimeType(),
                'name' => $item->getClientOriginalName(),
                'tempName' => $fileUploadService->storeTempFile(FileUploadService::OUTSIDE_CONTENT, $item, $temporaryFolder),
                'tempFolder' => $temporaryFolder
            ];
        };

        $uploadedFiles = [];
        foreach ($files as $file) {
            if (!is_array($file)) {
                $file = [$file];
            }
            foreach ($file as $subFile) {
                $uploadedFiles[] = $mapFiles($subFile);
            }
        }
        return $this->json(compact('uploadedFiles'));
    }

    /**
     * @throws Exception
     * @throws NonUniqueResultException
     * @throws NoResultException
     */
    #[Route('/api/stores/export', name: 'api_store_export', methods: 'POST')]
    public function export(Request $request)
    {
        /** @var QueryBuilder $qb */
        [
            'qb' => $qb
        ] = $this->getBaseResults($request);
        $exportQb = clone $qb;
        $distinct = sp_dql_has_distinct($qb->getQuery()) ? 'DISTINCT' : '';
        $rowCount = (int)$qb->select("COUNT($distinct {$this->alias}.id)")->getQuery()->getSingleScalarResult();
        if ($rowCount === 0) {
            throw new NoResultException();
        }

        $attributeInformation = $this->extractImportExportAttributeInformation();

        $query = $this->entityManager->createQuery($exportQb->getDQL());
        sp_dql_apply_other_parameters($exportQb, $query);
        $exportService = (new ExportService())
            ->createWorksheet('Stores');
        $exportService->setDefaultSheetProperties('Store export', $this->getUser());
        $exportService->getSheet()
            ->getDefaultStyle()
            ->applyFromArray(PhpSpreadsheetConstants::DEFAULT_FONT)
            ->getAlignment()
            ->setWrapText(true);
        $counter = 1;
        $iterator = $query->toIterable();
        $exportService->writeRow(array_column($attributeInformation, 'columnName'));
        foreach ($iterator as $row) {
            $counter++;
            $rowValues = [];
            foreach ($attributeInformation as ['getterFunction' => $getterFunction, 'exportProcessor' => $exportProcessor]) {
                $value = $row->{$getterFunction}();
                if ($exportProcessor !== null) {
                    $value = $exportProcessor($value);
                }
                $rowValues[] = $value;
            }
            $exportService->writeRow($rowValues, $counter);
        }
        $exportService->applyStyleToRow(1, array_merge(PhpSpreadsheetConstants::HEADING, PhpSpreadsheetConstants::HEADING_FILL));
        $exportService->applyAutoWidth(1);
        $document = $exportService->generateDocument();
        return ExportService::generateResponse($document, 'Export');
    }

    private function extractImportExportAttributeInformation(): array
    {
        $concernedClass = Store::class;

        $reflectionClass = new ReflectionClass($concernedClass);
        $properties = $reflectionClass->getProperties();
        $storeMetadata = $this->entityManager->getClassMetadata(Store::class);

        $extractedProperties = [];
        foreach ($properties as $property) {
            $importExportAttributes = $property->getAttributes(ImportExportAttribute::class);
            if (count($importExportAttributes) !== 1) {
                continue;
            }
            /** @var ImportExportAttribute $importExportAttribute */
            $importExportAttribute = reset($importExportAttributes)->newInstance();
            $columnName = $importExportAttribute->getColumnName();
            $propertyName = $property->getName();
            if ($columnName === null) {
                $columnName = sp_string_normalize_camel_snake_kebab_to_words($propertyName);
            }

            $getterFunction = $importExportAttribute->getGetter();
            if ($getterFunction === null) {
                $getterFunction = sp_getter($propertyName);
            }
            if ($getterFunction !== null && !method_exists($concernedClass, $getterFunction)) {
                throw new \RuntimeException('Method ' . $getterFunction . ' does not exist on ' . $concernedClass);
            }
            $getterReflectionMethod = new \ReflectionMethod($concernedClass, $getterFunction);
            $exportProcessor = null;
            if ($getterReflectionMethod->getReturnType()?->getName() === 'bool') {
                $exportProcessor = static function (?bool $value) {
                    return match ($value) {
                        null => '',
                        false => 'No',
                        true => 'Yes',
                    };
                };
            }

            $setterFunction = $importExportAttribute->getSetter();
            if ($setterFunction === null) {
                $setterFunction = sp_setter($propertyName);
            } elseif (!method_exists($concernedClass, $setterFunction)) {
                throw new \RuntimeException('Method ' . $setterFunction . ' does not exist on ' . $concernedClass);
            }
            $setterReflectionMethod = new \ReflectionMethod($concernedClass, $setterFunction);
            $importProcessor = null;
            $setterParameter = $setterReflectionMethod->getParameters()[0];
            if ($setterParameter->getType()?->getName() === 'bool') {
                $allowsNulls = $setterParameter->getType()?->allowsNull() ?? false;
                $importProcessor = static function ($mixed, ?string $value) use ($allowsNulls) {
                    return match (strtolower($value ?? '')) {
                        'no' => false,
                        'yes' => true,
                        default => $allowsNulls ? null : false
                    };
                };
            }
            if (!empty($importProcessors = $property->getAttributes(ImportProcessorAttribute::class))) {
                /** @var ImportProcessorAttribute $importProcessor */
                $importProcessorAttr = reset($importProcessors)->newInstance();
                $importProcessor = static function (self $_this, mixed $value) use ($importProcessorAttr): mixed {
                    if ($importProcessorAttr->getService()) {
                        return $_this->{$importProcessorAttr->getService()}->{$importProcessorAttr->getFunction()}($value);
                    }
                    return $_this->{$importProcessorAttr->getFunction()}($value);
                };
            }

            $dbColumnName = array_flip($storeMetadata->fieldNames)[$propertyName] ?? null;
            if ($dbColumnName === null) {
                $array = array_values($storeMetadata->associationMappings[$propertyName]['joinColumnFieldNames'] ?? []);
                $dbColumnName = reset($array);
            }

            $isIdentifier = $importExportAttribute->getIsIdentifierField();

            $extractedProperties[] = compact('columnName', 'propertyName', 'getterFunction', 'setterFunction', 'dbColumnName', 'isIdentifier', 'exportProcessor', 'importProcessor');
        }
        return $extractedProperties;
    }

    /**
     * Imports a list of stores from an excel sheet
     * * **Notes:**
     * 1. The function imports data from multiple worksheets
     * 1. It is assumed that the each worksheet's columns are in the same order as the store properties
     * 2. No data will be written to the database if an error is encountered during the import
     * 3. The import will only process up to the number of properties that store has (25) and ignore any columns past that point
     * * **Column Properties:**
     * 1. name 
     * 2. brand
     * 3. industry
     * 4. status
     * 5. apiId
     * 6. facebookVerified
     * 7. facebookId
     * 8. facebookPageName
     * 9. facebookUrl
     * 10. googleVerified
     * 11. googlePlaceId
     * 12. googleLocationId
     * 13. googleMapsUrl
     * 14. tripAdvisorVerified
     * 15. tripAdvisorId
     * 16. tripAdvisorPartnerPropertyId
     * 17. tripAdvisorUrl
     * 18. zomatoVerified 
     * 19. zomatoId
     * 20. zomatoUrl
     * 21. instagramVerified
     * 22. instagramId
     * 23. instagramUrl
     * 24. latitude
     * 25. longitude
     */
    #[Route('/api/stores/import/process', name: 'api_store_process_import', methods: 'POST')]
    public function import(Request $request, ValidatorInterface $validator): StreamedResponse|JsonResponse
    {
        /*TODO: Fix docker memory leak where data isn't cleared after each run */

        $folder = $request->get('folder');
        $fileName = $request->get('fileName');
        $filePath = FileUploadService::CONTENT_PATH . "/temp-uploads/$folder/$fileName";
        $attributeInformation = $this->extractImportExportAttributeInformation();

        $this->importService->loadDocument($filePath);

        /* The number of stores that will be batched into a single flush to the database */
        $batchSize = 100;

        foreach ($this->importService->iterateSheets() as $sheetIndex => $sheet) {
            $tableErrors = [];

            $storeList = [];

            foreach ($this->importService->toIterator((int) $sheetIndex) as $row) {
                $store = new Store();
                $rowKeys = array_keys((array) $row);

                /* Loop through Store meta data to find and call the correct setters on $store object */
                foreach ($attributeInformation as $attributeIndex => $attribute) {
                    if (!isset($rowKeys[$attributeIndex])) continue;

                    $data = $row[$rowKeys[$attributeIndex]];

                    $importProcessor = $attribute["importProcessor"];

                    if ($importProcessor) $data = $importProcessor($this, $data);

                    $propertyName = ucfirst($attribute["propertyName"]);
                    $setterFunction = $attribute["setterFunction"] ?? "set$propertyName";

                    $store->{$setterFunction}($data);
                }

                /* Run validation on store object and extract column errors into deliminated list */
                $rowErrors = sp_extract_errors_as_string($validator->validate($store));

                if ($rowErrors) $tableErrors[] = $rowErrors;
                else if (!$tableErrors) $storeList[] = $store;
            }

            /* Only perform database updates and insertions if there were no table errors encountered 
                while parsing the imported store data */
            if (!$tableErrors) {
                foreach ($storeList as $storeIndex => $store) {
                    $store->setDateCreated(new \DateTime);
                    $this->entityManager->persist($store);

                    /* After a batchSize number of interations, write the pending entity changes to the database and clear entity manager's memory of those
                    objects */
                    if (($storeIndex + 1) % $batchSize === 0) {
                        $this->entityManager->flush();
                        $this->entityManager->clear();
                    }
                }

                /* Write any remaning store objects in the current batch and clear entity manager */
                $this->entityManager->flush();
                $this->entityManager->clear();
            }
        }

        /* Clear spreadsheet data from memory */
        $this->importService->unsetDocument();

        return $this->json($tableErrors);
    }
}
