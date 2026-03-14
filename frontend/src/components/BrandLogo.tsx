import { BRAND_LOGOS } from '../constants/brandLogos';

type BrandLogoProps = {
  brand?: string | null;
  className?: string;
};

const normalizeBrandKey = (brand?: string | null) => {
  if (!brand) return '';
  return brand
    .trim()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
};

const resolveBrandMeta = (key: string) => {
  if (!key) return null;
  if (key.includes('AMERICAN EXPRESS') || key === 'AMEX') {
    return { logo: BRAND_LOGOS.AMERICAN_EXPRESS, title: 'American Express' };
  }
  if (key.includes('MASTERCARD') || key === 'MASTER') {
    return { logo: BRAND_LOGOS.MASTERCARD, title: 'Mastercard' };
  }
  if (key.includes('VISA')) {
    return { logo: BRAND_LOGOS.VISA, title: 'Visa' };
  }
  if (key.includes('ELO')) {
    return { logo: BRAND_LOGOS.ELO, title: 'Elo' };
  }
  if (key.includes('DINERS CLUB') || key === 'DINERS') {
    return { logo: BRAND_LOGOS.DINERS_CLUB, title: 'Diners Club' };
  }
  if (key.includes('DISCOVER')) {
    return { logo: BRAND_LOGOS.ALELO, title: 'Alelo' };
  }
  if (key.includes('JCB')) {
    return { logo: BRAND_LOGOS.JCB, title: 'JCB' };
  }
  if (key.includes('AURA')) {
    return { logo: BRAND_LOGOS.AURA, title: 'Aura' };
  }
  if (key.includes('CABAL')) {
    return { logo: BRAND_LOGOS.CABAL, title: 'Cabal' };
  }
  if (key.includes('BANESCARD')) {
    return { logo: BRAND_LOGOS.BANESCARD, title: 'Banescard' };
  }
  if (key.includes('SOROCRED')) {
    return { logo: BRAND_LOGOS.SOROCRED, title: 'Sorocred' };
  }
  if (key.includes('VEROCHEQUE')) {
    return { logo: BRAND_LOGOS.VEROCHEQUE, title: 'Verocheque' };
  }
  if (key.includes('ALELO')) {
    return { logo: BRAND_LOGOS.ALELO, title: 'Alelo' };
  }
  if (key.includes('SODEXO')) {
    return { logo: BRAND_LOGOS.SODEXO, title: 'Sodexo' };
  }
  if (key.includes('VR')) {
    return { logo: BRAND_LOGOS.VR, title: 'VR' };
  }
  if (key.includes('TICKET')) {
    return { logo: BRAND_LOGOS.TICKET, title: 'Ticket' };
  }
  if (key.includes('BEN')) {
    return { logo: BRAND_LOGOS.BEN, title: 'Ben' };
  }
  if (key.includes('GOOD CARD') || key === 'GOODCARD') {
    return { logo: BRAND_LOGOS.GOOD_CARD, title: 'Good Card' };
  }
  if (key.includes('POLICARD')) {
    return { logo: BRAND_LOGOS.POLICARD, title: 'Policard' };
  }
  if (key.includes('VALECARD')) {
    return { logo: BRAND_LOGOS.VALECARD, title: 'ValeCard' };
  }
  if (key.includes('UP BRASIL') || key === 'UPBRASIL' || key === 'UP') {
    return { logo: BRAND_LOGOS.UP_BRASIL, title: 'Up Brasil' };
  }
  if (key.includes('GREENCARD')) {
    return { logo: BRAND_LOGOS.GREENCARD, title: 'Greencard' };
  }
  if (key.includes('COOPERCARD')) {
    return { logo: BRAND_LOGOS.COOPERCARD, title: 'Coopercard' };
  }
  if (key.includes('CONVCARD')) {
    return { logo: BRAND_LOGOS.CONVCARD, title: 'Convcard' };
  }
  if (key.includes('CONVNET')) {
    return { logo: BRAND_LOGOS.CONVCARD, title: 'Convcard' };
  }
  if (key.includes('TRICARD')) {
    return { logo: BRAND_LOGOS.TRICARD, title: 'Tricard' };
  }
  if (key.includes('MAIS')) {
    return { logo: BRAND_LOGOS.MAIS, title: 'Mais' };
  }
  if (key.includes('NUTRICASH')) {
    return { logo: BRAND_LOGOS.NUTRICASH, title: 'Nutricash' };
  }
  if (key.includes('FLEX')) {
    return { logo: BRAND_LOGOS.FLEX, title: 'Flex' };
  }
  if (key.includes('BANRICARD')) {
    return { logo: BRAND_LOGOS.BANRICARD, title: 'Banricard' };
  }
  if (key.includes('UNIONPAY')) {
    return { logo: BRAND_LOGOS.UNIONPAY, title: 'UnionPay' };
  }
  if (key.includes('HIPERCARD')) {
    return { logo: BRAND_LOGOS.HIPERCARD, title: 'Hipercard' };
  }
  if (key.includes('VIDALINK') || key.includes('VIDA LINK')) {
    return { logo: BRAND_LOGOS.ALELO, title: 'Alelo' };
  }
  if (key.includes('PIX')) {
    return { logo: BRAND_LOGOS.PIX, title: 'Pix' };
  }
  return null;
};

export const BrandLogo = ({ brand, className }: BrandLogoProps) => {
  const key = normalizeBrandKey(brand);
  const meta = resolveBrandMeta(key);
  const displayName = meta?.title ?? brand?.trim() ?? '—';

  if (!meta?.logo) {
    return <span title={displayName}>{displayName}</span>;
  }

  return (
    <img
      src={meta.logo}
      alt={displayName}
      title={displayName}
      className={className}
      style={{ height: 12, objectFit: 'contain' }}
    />
  );
};
