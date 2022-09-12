<?php

namespace App\Services;

use Exception;
use PhpOffice\PhpSpreadsheet\Cell\Cell;
use PhpOffice\PhpSpreadsheet\Cell\Coordinate;
use PhpOffice\PhpSpreadsheet\Reader\Xlsx as Reader;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Worksheet\RowCellIterator;
use PhpOffice\PhpSpreadsheet\Worksheet\Worksheet;

class ImportService
{
    private ?Spreadsheet $document;
    private int $activeIndex = 0;
    private ?string $originalPath;
    private ?string $downloadedPath;

    /**
     * @throws \PhpOffice\PhpSpreadsheet\Reader\Exception
     */
    public function loadDocument(string $path): self
    {
        $this->originalPath = $path;
        $this->downloadedPath = $path;
        if (filter_var($path, FILTER_VALIDATE_URL)) {
            $filename = sp_unique_string_based_on_uniqid('import-service', true) . '-' . basename($path);
            $temporaryFilePath = sys_get_temp_dir() . DIRECTORY_SEPARATOR . $filename;
            $file = file_get_contents($path);
            if ($file === false) {
                throw new \RuntimeException('Could not download file path');
            }
            file_put_contents($temporaryFilePath, $file);
            $this->downloadedPath = $temporaryFilePath;
        }
        $reader = new Reader();
        $reader = $reader
            ->setReadDataOnly(false)
            ->setReadEmptyCells(false);
        $this->document = $reader->load($this->downloadedPath);
        return $this;
    }

    public function unsetDocument()
    {
        $this->document->disconnectWorksheets();
        $this->document = null;
        unset($this->document);
        gc_collect_cycles();
    }

    public function iterateSheets(): \Generator
    {
        foreach ($this->document->getWorksheetIterator() as $worksheet) {
            yield $worksheet;
        }
    }

    public function getSheetNames(): array
    {
        return $this->document->getSheetNames();
    }

    /**
     * @throws \PhpOffice\PhpSpreadsheet\Exception
     */
    public function getRow(int $rowToRead, int $activeIndex = null, bool $trim = true): array
    {
        $this->validateDocument();
        $worksheet = $this->document->setActiveSheetIndex($this->getActiveIndex($activeIndex));
        $highestColumn = $worksheet->getHighestColumn();
        $highestColumnIndex = Coordinate::columnIndexFromString($highestColumn);
        $row = [];
        for ($col = 1; $col <= $highestColumnIndex; ++$col) {
            $value = $worksheet->getCellByColumnAndRow($col, $rowToRead)->getValue();
            $row[] = $trim && is_string($value) ? trim($value) : $value;
        }
        return $row;
    }

    public function toArray(
        bool $text = true,
        int $activeIndex = null,
        bool $headers = false,
        int $headerRow = 1,
        bool $stripBlank = false,
        bool $trim = true
    ): array {
        $this->validateDocument();
        $worksheet = $this->document->setActiveSheetIndex($this->getActiveIndex($activeIndex));
        $sheetRows = $worksheet->toArray(null, true, true);
        if ($trim) {
            foreach ($sheetRows as &$sheetRow) {
                $sheetRow = array_map(static fn ($value) => is_string($value) ? trim($value) : $value, $sheetRow);
            }
            unset($sheetRow);
        }
        if ($stripBlank) {
            $sheetRows = array_filter($sheetRows, static fn ($item) => count(array_filter($item)) > 0);
        }
        if ($headers === true) {
            $rows = [];
            foreach ($sheetRows as $key => $row) {
                if ($key >= $headerRow) {
                    $rows[] = array_combine($sheetRows[$headerRow - 1], $row);
                }
            }
            return $rows;
        }
        return $sheetRows;
    }

    /**
     * @throws \PhpOffice\PhpSpreadsheet\Exception
     */
    public function toIterator(
        int $activeIndex = null,
        int $headerRow = 1,
        bool $trimBlank = true,
        bool $stripBlank = false
    ): \Generator {
        $this->validateDocument();
        $worksheet = $this->document->setActiveSheetIndex($this->getActiveIndex($activeIndex));
        $iterator = $worksheet->getRowIterator($headerRow);
        $headerRowValues = [];
        $getRow = static fn (RowCellIterator $row) => array_map(static fn (Cell $cell) => $cell->getValue(), iterator_to_array($row));
        $trimRow = static fn (array $row) => array_map(static fn ($value) => is_string($value) ? trim($value) : $value, $row);
        $stripRow = static fn (array $row) => array_filter($row, static fn ($item) => count(array_filter($item)) > 0);
        foreach ($iterator as $item) {
            $row = $getRow($item->getCellIterator());
            if ($trimBlank) {
                $row = $trimRow($row);
            }
            if ($stripBlank) {
                $row = $stripRow($row);
            }
            if (empty($row)) {
                continue;
            }
            if (empty($headerRowValues)) {
                $headerRowValues = $row;
                continue;
            }

            $row = array_combine($headerRowValues, $row);
            unset($row['']);
            yield $row;
        }
    }

    public function convertWorksheetToArrayOfClass(
        string $class,
        array $additionalParams = [],
        bool $passHeadingsIntoConstructor = true,
        bool $shipHeading = true,
        int $activeIndex = null,
        bool $stripBlank = false
    ): array {
        $asArray = $this->toArray(true, $activeIndex, false, 1, $stripBlank);

        if ($shipHeading) {
            $headings = array_splice($asArray, 0, 1)[0];
        } else {
            $headings = $asArray[0];
        }
        foreach ($asArray as &$item) {
            if ($passHeadingsIntoConstructor) {
                $item = new $class($item, $headings, ...$additionalParams);
            } else {
                $item = new $class($item, ...$additionalParams);
            }
        }
        return $asArray;
    }

    /**
     * @throws \RuntimeException
     */
    private function validateDocument(): void
    {
        if (!$this->document) {
            throw new \RuntimeException("No document");
        }
    }


    public function getActiveIndex(int $activeIndex = null): ?int
    {
        if ($activeIndex === null) {
            $activeIndex = $this->activeIndex === 0 ? $this->activeIndex : $this->activeIndex - 1;
        }
        return $activeIndex;
    }

    public function getDocument(): Spreadsheet
    {
        return $this->document;
    }

    public function getOriginalPath(): ?string
    {
        return $this->originalPath;
    }

    public function getDownloadedPath(): ?string
    {
        return $this->downloadedPath;
    }
}
