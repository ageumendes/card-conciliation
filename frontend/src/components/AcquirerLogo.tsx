import { ACQUIRER_LOGOS } from '../constants/acquirerLogos';

type AcquirerLogoProps = {
  acquirer?: string | null;
  className?: string;
};

const normalizeAcquirerKey = (acquirer?: string | null) => {
  if (!acquirer) return '';
  return acquirer.trim().toUpperCase();
};

export const AcquirerLogo = ({ acquirer, className }: AcquirerLogoProps) => {
  const key = normalizeAcquirerKey(acquirer);
  const logo =
    key.includes('CIELO')
      ? ACQUIRER_LOGOS.CIELO
      : key.includes('SIPAG')
        ? ACQUIRER_LOGOS.SIPAG
        : key.includes('SICREDI')
          ? ACQUIRER_LOGOS.SICREDI
          : null;
  const displayName = acquirer?.trim() || '—';

  if (!logo) {
    return <span title={displayName}>{displayName}</span>;
  }

  return (
    <img
      src={logo}
      alt={displayName}
      title={displayName}
      className={className}
      style={{ height: 22, objectFit: 'contain' }}
    />
  );
};
