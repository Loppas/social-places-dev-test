<?php

namespace App\Entity;

use App\Attributes\ImportExportAttribute;
use App\Attributes\ImportProcessorAttribute;
use App\Enums\StoreStatus;
use App\Services\StoreService;
use App\Traits\Entity\HasDateCreated;
use App\Traits\Entity\HasDateUpdated;
use App\Traits\Entity\HasId;
use Doctrine\ORM\Mapping as ORM;
use Symfony\Bridge\Doctrine\Validator\Constraints\UniqueEntity;
use Symfony\Component\Validator\Constraints as Assert;
use Symfony\Component\Validator\Mapping\CascadingStrategy;


#[ORM\Entity]
#[UniqueEntity(['apiId'])]
class Store
{
    use HasId;
    use HasDateCreated;
    use HasDateUpdated;

    #[ORM\Column(type: 'string', length: 255, unique: true)]
    #[Assert\Length(min: 2, max: 255)]
    #[Assert\NotNull]
    #[ImportExportAttribute]
    private string $name;

    #[ORM\ManyToOne(Brand::class, cascade: ['persist'], fetch: 'LAZY')]
    #[ORM\JoinColumn('brand_id', 'id')]
    #[ImportExportAttribute(getter: 'getBrandName')]
    #[ImportProcessorAttribute('discoverBrandByName', 'storeService')]
    private Brand $brand;

    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    #[Assert\Length(min: 2, max: 255)]
    #[ImportExportAttribute]
    private ?string $industry;

    #[ORM\Column(type: 'integer', nullable: false, enumType: StoreStatus::class)]
    #[ImportExportAttribute(getter: 'getStatusName', setter: 'setStatusFromName')]
    private StoreStatus $status;

    #[ORM\Column(type: 'string', length: 50, unique: true, nullable: false)]
    #[ImportExportAttribute('API ID', isIdentifierField: true)]
    #[Assert\NotBlank]
    private ?string $apiId;

    #[ORM\Column(type: 'boolean', nullable: false, options: ['default' => false])]
    #[ImportExportAttribute]
    private bool $facebookVerified = false;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute]
    private ?string $facebookId;

    #[ORM\Column(type: 'string', length: 255, nullable: true)]
    #[Assert\Length(max: 255)]
    #[ImportExportAttribute]
    private ?string $facebookPageName;

    #[ORM\Column(type: 'string', length: 1000, nullable: true)]
    #[Assert\Length(max: 1000)]
    #[ImportExportAttribute('Facebook URL')]
    private ?string $facebookUrl;

    #[ORM\Column(type: 'boolean', nullable: false, options: ['default' => false])]
    #[ImportExportAttribute]
    private bool $googleVerified = false;

    #[ORM\Column(type: 'string', length: 50, nullable: true)]
    #[Assert\Length(max: 50)]
    #[ImportExportAttribute]
    private ?string $googlePlaceId;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute]
    private ?string $googleLocationId;

    #[ORM\Column(type: 'string', length: 1000, nullable: true)]
    #[Assert\Length(max: 1000)]
    #[ImportExportAttribute('Google MAP URL')]
    private ?string $googleMapsUrl;

    #[ORM\Column(type: 'boolean', nullable: false, options: ['default' => false])]
    #[ImportExportAttribute('TripAdvisor Verified')]
    private bool $tripAdvisorVerified = false;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute('TripAdvisor Id')]
    private ?string $tripAdvisorId;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute('TripAdvisor Partner Property Id')]
    private ?string $tripAdvisorPartnerPropertyId;

    #[ORM\Column(type: 'string', length: 1000, nullable: true)]
    #[Assert\Length(max: 1000)]
    #[ImportExportAttribute('TripAdvisor URL')]
    private ?string $tripAdvisorUrl;

    #[ORM\Column(type: 'boolean', nullable: false, options: ['default' => false])]
    #[ImportExportAttribute]
    private bool $zomatoVerified = false;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute]
    private ?string $zomatoId;

    #[ORM\Column(type: 'string', length: 1000, nullable: true)]
    #[Assert\Length(max: 1000)]
    #[ImportExportAttribute('Zomato URL')]
    private ?string $zomatoUrl;

    #[ORM\Column(type: 'boolean', nullable: false, options: ['default' => false])]
    #[ImportExportAttribute]
    private bool $instagramVerified = false;

    #[ORM\Column(type: 'string', length: 25, nullable: true)]
    #[Assert\Length(max: 25)]
    #[ImportExportAttribute]
    private ?string $instagramId;

    #[ORM\Column(type: 'string', length: 1000, nullable: true)]
    #[Assert\Length(max: 1000)]
    #[ImportExportAttribute('Instagram URL')]
    private ?string $instagramUrl;

    #[ORM\Column(type: 'float', precision: 12, nullable: true)]
    #[ImportExportAttribute]
    private ?float $latitude;

    #[ORM\Column(type: 'float', precision: 12, nullable: true)]
    #[ImportExportAttribute]
    private ?float $longitude;

    public function __construct()
    {
        $this->status = StoreStatus::OPEN;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function setName(string $name): void
    {
        $this->name = $name;
    }

    public function getBrand(): Brand
    {
        return $this->brand;
    }

    public function setBrand(Brand $brand): void
    {
        $this->brand = $brand;
    }

    public function getBrandName(): string
    {
        return $this->brand->getName();
    }

    public function getIndustry(): ?string
    {
        return $this->industry;
    }

    public function setIndustry(?string $industry): void
    {
        $this->industry = $industry;
    }

    public function getStatus(): StoreStatus
    {
        return $this->status;
    }

    public function setStatus(StoreStatus $status): void
    {
        $this->status = $status;
    }

    public function getStatusName(): string
    {
        return $this->status->getName();
    }

    public function setStatusFromName(string $statusName): void
    {
        try {
            $this->status = StoreStatus::fromName($statusName);
        } catch (\Exception $e) {
        }
    }

    public function getApiId(): string
    {
        return $this->apiId;
    }

    public function setApiId(?string $apiId): void
    {
        $this->apiId = $apiId;
    }

    public function getFacebookVerified(): bool
    {
        return $this->facebookVerified;
    }

    public function setFacebookVerified(bool $facebookVerified): void
    {
        $this->facebookVerified = $facebookVerified;
    }

    public function getFacebookId(): ?string
    {
        return $this->facebookId;
    }

    public function setFacebookId(?string $facebookId): void
    {
        $this->facebookId = $facebookId;
    }

    public function getFacebookPageName(): ?string
    {
        return $this->facebookPageName;
    }

    public function setFacebookPageName(?string $facebookPageName): void
    {
        $this->facebookPageName = $facebookPageName;
    }

    public function getFacebookUrl(): ?string
    {
        return $this->facebookUrl;
    }

    public function setFacebookUrl(?string $facebookUrl): void
    {
        $this->facebookUrl = $facebookUrl;
    }

    public function getGoogleVerified(): bool
    {
        return $this->googleVerified;
    }

    public function setGoogleVerified(bool $googleVerified): void
    {
        $this->googleVerified = $googleVerified;
    }

    public function getGooglePlaceId(): ?string
    {
        return $this->googlePlaceId;
    }

    public function setGooglePlaceId(?string $googlePlaceId): void
    {
        $this->googlePlaceId = $googlePlaceId;
    }

    public function getGoogleLocationId(): ?string
    {
        return $this->googleLocationId;
    }

    public function setGoogleLocationId(?string $googleLocationId): void
    {
        $this->googleLocationId = $googleLocationId;
    }

    public function getGoogleMapsUrl(): ?string
    {
        return $this->googleMapsUrl;
    }

    public function setGoogleMapsUrl(?string $googleMapsUrl): void
    {
        $this->googleMapsUrl = $googleMapsUrl;
    }

    public function getTripAdvisorVerified(): bool
    {
        return $this->tripAdvisorVerified;
    }

    public function setTripAdvisorVerified(bool $tripAdvisorVerified): void
    {
        $this->tripAdvisorVerified = $tripAdvisorVerified;
    }

    public function getTripAdvisorId(): ?string
    {
        return $this->tripAdvisorId;
    }

    public function setTripAdvisorId(?string $tripAdvisorId): void
    {
        $this->tripAdvisorId = $tripAdvisorId;
    }

    public function getTripAdvisorPartnerPropertyId(): ?string
    {
        return $this->tripAdvisorPartnerPropertyId;
    }

    public function setTripAdvisorPartnerPropertyId(?string $tripAdvisorPartnerPropertyId): void
    {
        $this->tripAdvisorPartnerPropertyId = $tripAdvisorPartnerPropertyId;
    }

    public function getTripAdvisorUrl(): ?string
    {
        return $this->tripAdvisorUrl;
    }

    public function setTripAdvisorUrl(?string $tripAdvisorUrl): void
    {
        $this->tripAdvisorUrl = $tripAdvisorUrl;
    }

    public function getZomatoVerified(): bool
    {
        return $this->zomatoVerified;
    }

    public function setZomatoVerified(bool $zomatoVerified): void
    {
        $this->zomatoVerified = $zomatoVerified;
    }

    public function getZomatoId(): ?string
    {
        return $this->zomatoId;
    }

    public function setZomatoId(?string $zomatoId): void
    {
        $this->zomatoId = $zomatoId;
    }

    public function getZomatoUrl(): ?string
    {
        return $this->zomatoUrl;
    }

    public function setZomatoUrl(?string $zomatoUrl): void
    {
        $this->zomatoUrl = $zomatoUrl;
    }

    public function getInstagramVerified(): bool
    {
        return $this->instagramVerified;
    }

    public function setInstagramVerified(bool $instagramVerified): void
    {
        $this->instagramVerified = $instagramVerified;
    }

    public function getInstagramId(): ?string
    {
        return $this->instagramId;
    }

    public function setInstagramId(?string $instagramId): void
    {
        $this->instagramId = $instagramId;
    }

    public function getInstagramUrl(): ?string
    {
        return $this->instagramUrl;
    }

    public function setInstagramUrl(?string $instagramUrl): void
    {
        $this->instagramUrl = $instagramUrl;
    }

    public function getLatitude(): ?float
    {
        return $this->latitude;
    }

    public function setLatitude(mixed $latitude): void
    {
        if (is_string($latitude)) {
            $latitude = (float)$latitude;
        }
        $this->latitude = $latitude;
    }

    public function getLongitude(): ?float
    {
        return $this->longitude;
    }

    public function setLongitude(mixed $longitude): void
    {
        if (is_string($longitude)) {
            $longitude = (float)$longitude;
        }
        $this->longitude = $longitude;
    }
}
